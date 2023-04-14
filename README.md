# ETLer

mongodb： 1、抽取 chang stream；2、自动全量同步；3、自动重试

### 数据发送
发送给处理服务器的数据格式如下 
```
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
3、接口需要在10s内返回，如果失败，1min后会自动重试，无需返回任务数据     
4、接口请求的顺序很重要，处理服务器需要按照顺序处理所有的接口请求    
5、Ns 只有在Sync时才有意义    
6、Data 中的数据一次最多1000条，也是需要严格按照顺序处理所有的数据   
7、bson.Raw 表示的原始的bson数据，Sync是Document数据，ChangeStream是
Event数据，处理服务器根据情况自行解析即可   


### 服务运行
1、服务具有自动重启能力   
2、服务有自己的缓存，短暂的停服后重启，会自动接上之前的流程   
3、数据的处理服务器即使较长时间停服，也不会影响本服务的工作，
当处理服务器恢复后，会自动恢复数据的发送    
4、目前暂没有删除抽取的ChangeStream缓存   
5、修改配置，重启后，会自动对新添加的Collection进行全量，对于之前配置的 
而本次未配置的Collection，会自动停止与其相关的所有Sync或者ChangeStream
任务

