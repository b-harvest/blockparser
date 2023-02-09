package main

import (
	"fmt"
	"os"
	"strings"

	"chart/cmd"
	//"github.com/cosmosquad-labs/blockparser/cmd"
)

func main() {
	if strings.HasPrefix(os.Args[1], "rpc") {
		fmt.Println(os.Args)
		if err := cmd.RPCParserCmd().Execute(); err != nil {
			os.Exit(1)
		}
	} else if strings.HasPrefix(os.Args[1], "chart") {
		if err := cmd.BlockParserCmdForChart().Execute(); err != nil {
			os.Exit(1)
		}
	} else if strings.HasPrefix(os.Args[1], "consensus") {
		if err := cmd.ConsensusParserCmd().Execute(); err != nil {
			os.Exit(1)
		}
	} else {
		if err := cmd.BlockParserCmd().Execute(); err != nil {
			os.Exit(1)
		}
	}
}
