package Connect

import (
	"HummingbirdDS/Flake"
	"HummingbirdDS/KvServer"
	"HummingbirdDS/Persister"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

// 第一版的创建连接还是需要服务器之间都确定对方的IP，客户端也需要知道服务器的地址
// TODO 后面可以改成类似redis，通过消息的交互得到其他节点的存在

type ServerConfig struct {
	peers     []*rpc.Client        // 表示其他几个服务器的连接句柄
	me        uint64               // 后面改成全局唯一ID
	nservers  int                  // 表示一共有多少个服务器
	kvserver  *KvServer.RaftKV     // 一个raftkv实体
	persister *Persister.Persister // 持久化实体
	mu        sync.Mutex           // 用于保护本结构体的变量
	// 不设置成大写没办法从配置文件中读出来
	Maxreries      int      `json:"maxreries"` 			// 超时重连最大数
	ServersAddress []string `json:"servers_address"`   	// 读取配置文件中的其他服务器地址
	MyPort         string   `json:"myport"`    			// 自己的端口号
	TimeOutEntry   int		`json:"timeout_entry"`		// connectAll中定义的重传超时间隔 单位为毫秒
}

/*
 * @brief: 拿到其他服务器的地址，分别建立RPC连接
 * @return:三种返回类型：超时;HttpError;成功
 */
// peers出现了条件竞争；修复，把服务启动在连接完成以后
func (cfg *ServerConfig) connectAll() error {
	sem := make(semaphore, cfg.nservers-1)
	sem_number := 0
	var HTTPError int32 = 0
	var TimeOut []int
	var timeout_mutex sync.Mutex

	servers_length := len(cfg.ServersAddress)
	for i := 0; i < servers_length; i++ {
		if atomic.LoadInt32(&HTTPError) > 0 {
			break
		}
		client, err := rpc.DialHTTP("tcp", cfg.ServersAddress[i])
		/*
		 * 这里返回值有三种情况:
		 * net.Dial返回error			： 重连
		 * http.ReadResponse返回		： HTTP报错
		 * 正常返回
		 */
		if err != nil {
			switch err.(type) {
			case *net.OpError: // 与库实现挂钩 不同步版本的标准库实现这里可能需要改动
				sem_number++
				// 网络出现问题我们有理由报错重试，次数上限为MAXRERRIES，每次间隔时间翻倍
				go func(index int) {
					defer sem.P(1)
					number := 0
					Timeout := cfg.TimeOutEntry
					for number < cfg.Maxreries {
						if atomic.LoadInt32(&HTTPError) > 0 {
							return
						}
						log.Printf("%s : Reconnecting for the %d time\n", cfg.ServersAddress[index], number+1)
						number++
						Timeout = Timeout * 2
						time.Sleep(time.Duration(Timeout) * time.Millisecond) // 倍增重连时长
						TempClient, err := rpc.DialHTTP("tcp", cfg.ServersAddress[index])
						if err != nil {
							switch err.(type) {
							case *net.OpError:
								// 继续循环就ok
								continue
							default:
								atomic.AddInt32(&HTTPError, 1)
								return
							}
						} else {
							// cfg.mu.Lock()
							// defer cfg.mu.Unlock()	// 没有协程会碰这个
							log.Printf("%d : 与%d 连接成功\n", cfg.me,cfg.ServersAddress[index])
							cfg.peers[index] = TempClient
							return
						}
					}
					// 只有循环cfg.maxreries遍没有结果以后才会跑到这里
					// 也就是连接超时
					timeout_mutex.Lock()
					defer timeout_mutex.Unlock()
					TimeOut = append(TimeOut, index) // 为了方便打日志
					return
				}(i)
				continue
			default:
				atomic.AddInt32(&HTTPError, 1)
			}
		} else {
			log.Printf("%d : 与%d 连接成功\n", cfg.me,cfg.ServersAddress[i])
			cfg.peers[i] = client
		}
	}
	// 保证所有的goroutinue跑完以后退出，即要么全部连接成功，要么报错
	sem.V(sem_number)

	TimeOutLength := len(TimeOut)
	if atomic.LoadInt32(&HTTPError) > 0 || TimeOutLength > 0 { // 失败以后释放连接
		for i := 0; i < servers_length; i++ {
			cfg.peers[i].Close() // 就算连接已经根本没建立进行close也只会返回ErrShutdown
		}
		if TimeOutLength > 0 {
			return ErrorInConnectAll(time_out)
		}
		return ErrorInConnectAll(http_error)
	} else {
		return nil	// 没有发生任何错误 成功
	}
}

func (cfg *ServerConfig) serverRegisterFun() {

	// 把RaftKv挂到RPC上
	err := rpc.Register(cfg.kvserver)
	if err != nil {
		// RPC会把全部函数中满足规则的函数注册，如果存在不满足规则的函数则会返回err
		log.Println(err.Error())
	}

	// 把Raft挂到RPC上
	err1 := rpc.Register(cfg.kvserver.GetRaft())
	if err1 != nil {
		log.Println(err1.Error())
	}

	// 通过函数把mathutil中提供的服务注册到http协议上，方便调用者可以利用http的方式进行数据传输
	rpc.HandleHTTP()

	// 在特定的端口进行监听
	listen, err2 := net.Listen("tcp", cfg.MyPort)
	if err2 != nil {
		log.Println(err2.Error())
	}
	go func() {
		// 调用方法去处理http请求
		http.Serve(listen, nil)
	}()
}

/*
 * @brief: 再调用这个函数的时候开始服务,
 * @return: 三种返回类型：路径解析错误;connectAll连接出现问题;成功
 */
func (cfg *ServerConfig) StartServer() error {
	var flag bool = false
	if len(ServerListeners) == 1 {
		// 正确读取配置文件 TODO 记得后面路径改一手
		flag = ServerListeners[0]("/home/lzl/go/src/HummingbirdDS/Config/server_config.json", cfg)
		if !flag {
			log.Println("File parser Error!")
			return ErrorInStartServer(parser_error)
		}
	} else {
		log.Println("ServerListeners Error!")
		// 这种情况只有在调用服务没有启动read_server_config.go的init函数时会出现
		return ErrorInStartServer(Listener_error)
	}

	// 这里初始化的原因是要让注册的结构体是后面运行的结构体
	cfg.kvserver = KvServer.StartKVServerInit(cfg.me, cfg.persister, 0)
	cfg.kvserver.StartRaftServer()

	cfg.serverRegisterFun()
	if err := cfg.connectAll(); err != nil {
		log.Println(err.Error())
		return ErrorInStartServer(connect_error)
	}
	cfg.kvserver.StartKVServer(cfg.peers)	// 启动服务
	log.Printf("%s : 连接成功 且服务以启动成功 \n" , cfg.MyPort)
	return nil
}

/*
 * @brief: 返回一个Config结构体，用于开始一个服务
 * @param: nservers这个集群需要的机器数，包括本身，而且每个服务器初始化的时候必须配置正确
 */
func CreatServer(nservers int) *ServerConfig {
	cfg := &ServerConfig{}

	cfg.nservers = nservers
	cfg.peers = make([]*rpc.Client, cfg.nservers-1) // 存储了自己以外的其他服务器的RPC封装
	cfg.me = Flake.GenSonyflake()                   // 全局ID需要传给raft层
	cfg.persister = Persister.MakePersister()

	return cfg
}

// --------------------------
// 使用Listener模式避免/Connect和/Config的环状引用
// 当然可能好的设计可以避免这样的问题，比如把读配置文件中的函数参数改成接口，用反射去推

type ServerListener func(filename string, cfg *ServerConfig) bool

var ServerListeners []ServerListener

func RegisterRestServerListener(l ServerListener) {
	ServerListeners = append(ServerListeners, l)
}

// --------------------------
