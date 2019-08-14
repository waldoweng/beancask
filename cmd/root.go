package cmd

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/common-nighthawk/go-figure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/waldoweng/beancask/storage"
	"github.com/waldoweng/beancask/wsgi"
)

type configS struct {
	Host          string
	Port          int
	MaxKeySize    int
	MaxValueSize  int
	CompactSize   int
	DataDir       string
	ActiveDataDir string
	LogDir        string
	LogLevel      string
}

var configFile string
var config configS

func printBanner() {
	fmt.Println("")
	figure.NewFigure("Beancask", "smslant", false).Print()
	fmt.Println("")
}

func initConfig() {
	log.SetFlags(log.LstdFlags)

	viper.SetConfigName("beancask")
	viper.AddConfigPath("$HOME/.beancask")

	err := viper.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Panicf("Fatal error reading config file: %s", err.Error())
		}
	}

	RootCmd.PersistentFlags().StringVar(&configFile, "config", "$HOME/.beancask/beancask.yaml", "config file")
	RootCmd.PersistentFlags().StringVarP(&config.Host, "host", "H", "0.0.0.0", "listening host")
	RootCmd.PersistentFlags().IntVarP(&config.Port, "port", "P", 3680, "listening port")

	viper.BindPFlag("Host", RootCmd.PersistentFlags().Lookup("host"))
	viper.BindPFlag("Port", RootCmd.PersistentFlags().Lookup("port"))

	viper.SetDefault("MaxKeySize", 1024)
	viper.SetDefault("MaxValueSize", 1024*1024)
	viper.SetDefault("CompactSize", 512*1024*1024)
	viper.SetDefault("DataDir", "/usr/local/var/beancask")
	viper.SetDefault("ActiveDataDir", "/usr/local/var/beancask")
	viper.SetDefault("LogDir", "/var/log/beancask")
	viper.SetDefault("LogLevel", "debug")
}

func init() {
	cobra.OnInitialize(printBanner)
}

// RootCmd the root command of cobra-based command line tool.
var RootCmd = &cobra.Command{
	Use:   "beancask-server",
	Short: "beancask is a bitcask-like key-value store",
	Long:  "a simple and powerful key-value store in bitcask model",
	Run: func(cmd *cobra.Command, args []string) {
		logFile, err := os.OpenFile(
			fmt.Sprintf("%s/beancask.log", viper.Get("LogDir").(string)),
			os.O_WRONLY|os.O_APPEND|os.O_SYNC|os.O_CREATE,
			0666)
		if err != nil {
			panic(err)
		}

		option := storage.CreateOption{
			MaxKeySize:    viper.Get("MaxKeySize").(int),
			MaxValueSize:  viper.Get("MaxValueSize").(int),
			CompactSize:   viper.Get("CompactSize").(int),
			DataDir:       viper.Get("DataDir").(string),
			ActiveDataDir: viper.Get("ActiveDataDir").(string),
			LogLevel:      viper.Get("LogLevel").(string),
			InfoLog:       log.New(logFile, "|info|", log.LstdFlags),
			DebugLog:      log.New(logFile, "|debug|", log.LstdFlags),
			ErrorLog:      log.New(logFile, "|error|", log.LstdFlags),
		}

		u := wsgi.Wsgi{
			Bitcask: storage.NewBitcask(option),
		}
		defer func() { u.Bitcask.Close() }()

		log.Printf("beancask server start. listening http://%s:%d\n", config.Host, config.Port)
		wsgi.SetUpRestfulService(u)
		log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%d", config.Host, config.Port), nil))
	},
}

// Execute execute the command
func Execute() {

	initConfig()

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
