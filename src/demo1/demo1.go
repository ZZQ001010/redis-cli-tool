package main

import (
	"bufio"
	"flag"
	"fmt"
	"gopkg.in/redis.v5"
	"io"
	"log"
	"os"
	"strings"
)

var (
	REDIS_CLUSTER_CLIENT *redis.ClusterClient

	redis_host string
	redis_port int
	file_path =""

	password string

	key_prefix string
	value =""

	sub_cmd string
)




func main() {
	cmd_exec(os.Args[1])
}

func init()  {

	initArgs()
}


func printDef()  {
	log.Printf("\n******************<这货不是一个小工具>****************\n" +
		"可以帮助你轻松的帮助你从redis单机导出导入数据，从rediscluster 导出导入数据\n" +
		"目前仅仅支持key-value格式数据，并且value为固定值,导入文件格式为key<换行符>" +
		"命令行参数介绍\n" +
		"-p       端口<redis 单机版使用>\n" +
		"-h		  主机ip<redis单机版使用直接写ip><rediscluster使用ip1:port1,ip2:port2的格式>\n" +
		"-f		  导入导出文件名<redis单机版使用><rediscluster使用>\n" +
		"-a		  认证口令<redis单机版使用><redis集群版使用>\n" +
		"-kp	  主键的前缀\n" +
		"--h	  提供帮助文档\n" +
		"--v	  工具版本\n" +
		"-val     value值")
}

/**
初始化参数
 */
func initArgs()  {
	if len(os.Args) <=1 {
		log.Fatal("请输入子命令: dump|import|clear|version|cluster_import|help")
	}
	flag.StringVar(&redis_host,"h","127.0.0.1","redis host")
	flag.IntVar(&redis_port,"p",6379,"redis port")
	flag.StringVar(&file_path,"f","","file path")
	flag.StringVar(&password,"a","","redis password")
	flag.StringVar(&key_prefix,"kp","CARD-","key prefix")
	flag.StringVar(&value,"val","d01","dcn num")

	flag.CommandLine.Parse(os.Args[2:])

	if os.Args[1]== "--h"{
		printDef()
		return
	}else if os.Args[1]=="--v" {
		printVersion()
	}

	if strings.HasPrefix(os.Args[1], "-"){
		log.Fatal("请输入子命令: dump|import|clear|version|cluster_import|help")

	}


}

// 命令执行者
func cmd_exec(sub_cmd string)  {
	switch sub_cmd {
	case "cluster_import":
		chk_file()
		handle_cluster_import()
	default:
		log.Fatal("程序现在仅仅支持cluster_import")
	}

}

func chk_value()  {
	if file_path == "" {
		log.Fatal("请使用-f参数指定导入导出的文件名")
	}
}

func handle_cluster_import()  {
	REDIS_CLUSTER_CLIENT = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:              strings.FieldsFunc(redis_host, func(r rune) bool {
			if r == ',' {
				return true
			}
			return false
		}),
		Password:           password,
	})

	defer  REDIS_CLUSTER_CLIENT.Close()
	res, err := REDIS_CLUSTER_CLIENT.Ping().Result()
	if err == nil {
		fmt.Println(res)
		fmt.Println("连接成功...")
	} else {
		fmt.Println(err)
		log.Fatal("连接redis-cluster 失败,请检查连接信息")
	}

	cluster_handle_import()
}

func chk_file() {
	if file_path == "" {
		log.Fatal("请使用-f参数指定导入导出的文件名")
	}
}



func printVersion()  {
	log.Printf("\n 1.0.0")
}

func cluster_handle_import() {

	pip:=REDIS_CLUSTER_CLIENT.Pipeline()
	defer  pip.Close()
	f, err := os.Open(file_path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n') //以'\n'为结束符读入一行
		//处理一行数据
		line = strings.Replace(line, "\n", "", -1)
		key:=key_prefix+line
		st:=pip.Set(key,value,0)
		print(st)
		if err != nil || io.EOF == err {
			break
		}
	}
  		res,err:=pip.Exec()
	if err==nil {
		log.Fatal(err)
	} else {
  		log.Println(res)
  		log.Println("导入完毕...销毁程序")
	}

}
