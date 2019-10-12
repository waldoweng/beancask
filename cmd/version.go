package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	RootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of beancask-server",
	Long:  `all software has versions. this is beancask-server's`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("beancask key value store v0.0.1 -- HEAD")
	},
}
