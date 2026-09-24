# ETLer

mongodb： 1、抽取 chang stream；2、自动全量同步；3、自动重试；4、自动恢复

### 服务运行

1、编译出可指定文件 etl   
2、etl -h 可查看运行命令，使用 etl pcjson 可以打印如下所示的配置文件，`// `开头的 key 是注释
```json
{
	"github.com/xpwu/go-tinyserver/http:serverConfig": {
		"Net": {
			"// Listen": "1、xxx.xxx.xxx.xxx:[0-9] 2、:[0-9] 3、pipe:[0-9] 4、unix:|xxx|xxx|xxx|xxx.socket:0",
			"Listen": "",
			"// MaxConnections": "-1:not limit",
			"MaxConnections": -1,
			"TLS": false,
			"TlsFile": {
				"// PrivateKeyPEMFile": "support relative path, must PEM encode data",
				"PrivateKeyPEMFile": "",
				"// CertPEMFile": "support relative path, must PEM encode data",
				"CertPEMFile": ""
			}
		},
		"// HostName": "leftmost match, []: allow all host name",
		"HostName": [],
		"// RootUri": "match_uri = RootUri + api.RegisterUri",
		"RootUri": "/"
	},
	"github.com/xpwu/ETLer/etl/config:watch": {
		"// Deployment": " Watching DB",
		"Deployment": {
			"uri": "",
			"user": "",
			"passwd": "",
			"maxconn": 2
		},
		"// Collections": "init the WatchCollections",
		"Collections": [
			{
				"DB": "",
				"Collection": ""
			}
		],
		"// SendToUrls": "send in order until successful",
		"SendToUrls": [
			"http://send/data/to"
		]
	},
	"github.com/xpwu/go-log/log:config": {
		"// level": "0:DEBUG; 1:INFO; 2:WARNING; 3:ERROR; 4:FATAL",
		"level": 0
	}
}
```

3、修改配置文件，并修改文件名为 config.json， (也可以是其他名称，需要在启动 etl 服务时，指定配置文件)，如下是一个示例
```json
{
	"github.com/xpwu/go-tinyserver/http:serverConfig": {
		"Net": {
			"Listen": "192.168.0.3:8080",
			"MaxConnections": -1,
			"TLS": false,
			"TlsFile": {
				"PrivateKeyPEMFile": "",
				"CertPEMFile": ""
			}
		},
		"HostName": [],
		"RootUri": "/api"
	},
	"github.com/xpwu/ETLer/etl/config:watch": {
		"Deployment": {
			"uri": "mongodb://192.168.1.11:27017,192.168.1.12:27017,192.168.1.13:27017/?replicaSet=rs0",
			"user": "root",
			"passwd": "rootpswd",
			"maxconn": 2
		},
		"Collections": [
			{
				"DB": "",
				"Collection": ""
			}
		],
		"// SendToUrls": "send in order until successful",
		"SendToUrls": [
			"192.168.0.4:8080/etlbeta", 
			"192.168.0.5:80/etl"
		]
	},
	"github.com/xpwu/go-log/log:config": {
		"// level": "0:DEBUG; 1:INFO; 2:WARNING; 3:ERROR; 4:FATAL",
		"level": 0
	}
}
```

4、执行 ./etl 启动服务，-h 可以看到命令帮助   
5、服务会在本地磁盘产生一个 bboltdb 目录，保存 etl 服务运行需要的数据

### 数据发送

发送给处理服务器的数据格式如下

```go
type Type byte

const (
	Sync Type = iota
	ChangeStream
)

type ns struct {
	DB   string
	Coll string
}

type Request struct {
	T Type
	// T == ChangeStream, Ns = {DB: "", Coll: ""}
	Ns ns
	Data []bson.Raw
}

```

1、服务器的接口需要保证幂等性   
2、同一时间只会有一个接口请求，要么是Sync，要么是ChangeStream     
3、接口需要在10s内返回，超时或者返回非http200都认为失败，成功时，无需返回任务接口数据   
4、会按照 "SendToUrls" 配置的接收服务器顺序请求，只要有一个成功，即为整体成功，成功后不再向剩下的目标服务器发送此条数据   
5、如果全部失败，15s后会自动从 "SendToUrls" 的第一个地址重试    
6、接口请求的顺序很重要，处理服务器需要按照请求的顺序处理所有的接口请求，否则会出现数据错乱    
7、Ns 只有在Sync时才有意义    
8、Data 中的数据一次最多1000条，也是需要严格按照顺序处理所有的数据   
9、bson.Raw 表示的原始的bson数据，Sync是Document数据，ChangeStream是Event数据，处理服务器根据情况自行解析即可   

### 服务访问

etl 提供 http(s) 协议的 API, 可以配置抽取的具体集合，数据格式为 json 格式。http:serverConfig 的配置项就是配置的此服务，
按照示例，此服务器的 url 前缀为 192.168.0.3:8080/api/，提供如下的接口，所有接口都是 post 且是幂等的，接口调用成功返回 http 200   
    
0、基本数据结构定义
```go
package x

type WatchInfo struct {
	DB         string
	Collection string
}
```
    
1、SetWatchCols  设置抽取的集合
```go
type setWcReq struct {
	Version    uint64        `json:"version"`
	WatchInfos []x.WatchInfo `json:"watchInfos"`
}

type setWcRes struct {
	OldVersion uint64 `json:"oldver"`
	NowVersion uint64 `json:"nowver"`
}
```
* version 为本次设置的版本号，返回的是本次设置之前的旧的版本号(oldver)与设置后最新的版本号(nowver)，
* 如果新的请求的 version 不大于之前的版本，本次设置不会生效，也不会对服务有任何影响，
* 通过 Res.NowVersion == setWcReq.Version && Res.OldVersion != setWcReq.Version 
可以判断本次调用是否生效(但是，调用者一般不用关注本次调用是否生效，只需要关注接口访问是否成功)

***config 中的 config:watch --- Collections 说明：*** 
* 配置文件中配置的抽取集合默认为 version = 0, 
* 每次重启服务时，都会用最新的 Collections配置去更新抽取集合，如果旧的版本为0，进行更新，如果旧的版本不会0，不会实际更新数据，
* api 设置的抽取集合，需要 version > 0 才会更新配置文件中设置的数据，api 一旦设置生效，配置文件的设置将不在生效(因为配置文件的设置版本为0)
      
2、GetWatchCols  获取当前设置的抽取集合
```go
type getWcRes struct {
	Version    uint64        `json:"version"`
	WatchInfos []x.WatchInfo `json:"watchInfos"`
}

// 请求数据为空json '{}'
type getWcReq struct {
}
```
      
3、ClearWC 清除所有的抽取配置
请求数据与响应数据都是空json  `{}`，清除后，如果不用 api 设置任何抽取数据，config:watch --- Collections 的数据将在下次重启时又生效
     
4、ForceSyncColl 强制全量同步某个指定的集合
```go
type syncCollectionReq struct {
	Collections []x.WatchInfo `json:"colls"`
}

type syncWcRes struct {
	Succeed bool
}
```
命令成功收到并处理即返回，不代表同步已经开始或已经完成，但只要返回了 succeed, 服务只要运行，后续一定保证能执行同步。   

5、ForceFullSync 强制全量同步所有抽取的集合   
请求数据与响应数据都是空json  `{}`，返回即代表命令收到并处理成功，不代表同步已经开始或已经完成，但只要返回了 succeed, 服务只要运行，后续一定保证能执行同步。
    
6、BackupDB  备份 etl 服务运行的本地db
```go
type backupReq struct {
}

type backupRes struct {
	// 相对于 etl 服务运行路径的相对路径及备份的文件名
	// 如果备份失败，返回 ""
	FileName string `json:"file_name"`
}
```
etl 运行会在磁盘建立本地数据库，用于保存 etl 运行的状态及缓存读取到的 change stream 等数据，可以定期备份并保存在异地，防止本地磁盘崩溃造成本地
数据的丢失而影响服务再次重启后的恢复时间，主要恢复工作耗费在全量同步阶段。即使没有备份数据，也不影响服务的正常重启，仅是多做一次全量同步延长了重启时间而已，
因为处理服务器是幂等的，不会对最后的结果造成影响。

### 主要特性

1、服务具有自动重试与自动恢复能力   
2、服务有自己的缓存，etl 停服后重启，会自动续上之前的流程   
3、数据的处理服务器即使较长时间停服，也不会影响本服务的工作，当处理服务器恢复后，会自动恢复数据的发送     


### 使用建议
1、对于一类抽取目标，应该只运行一个 etl 服务，不应对 etl 做热备   
2、通过接口设置需要抽取的集合，可以放在接口调用服务的启动阶段，因为 etl 接口的幂等性，接口的调用可以多台并发或重复请求     
3、每日备份一次 etl 的本地工作数据库   



