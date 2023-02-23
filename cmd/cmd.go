package cmd

import (
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	liquiditytypes "github.com/crescent-network/crescent/v4/x/liquidity/types"
	"github.com/spf13/cobra"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tendermint/budget/app"
	"github.com/tendermint/tendermint/state"
	"github.com/tendermint/tendermint/store"

	//liquiditytx "github.com/crescent-network/crescent/v4/x/liquidity/tx"

	_ "github.com/go-sql-driver/mysql"
	tmdb "github.com/tendermint/tm-db"
)

const (
	DSN      = "devops:passw0rd@tcp(51.195.63.75:3306)/crst_chart"
	BLOCK_DB = "data/blockstore_bk"
	STATE_DB = "data/state_bk"
)

func BlockParserCmdForChart() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "blockparser chart [chain-dir] [start-height] [end-height]",
		Args: cobra.ExactArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := args[1]
			startHeight, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("parse start-Height: %w", err)
			}
			endHeight, err := strconv.ParseInt(args[3], 10, 64)
			if err != nil {
				return fmt.Errorf("parse end-Height: %w", err)
			}

			blockDB, err := tmdb.NewGoLevelDBWithOpts(BLOCK_DB, dir, &opt.Options{
				ErrorIfMissing: true,
				ReadOnly:       true,
			})
			if err != nil {
				panic(err)
			}
			defer blockDB.Close()

			stateDB, err := tmdb.NewGoLevelDBWithOpts(STATE_DB, dir, &opt.Options{
				ErrorIfMissing: true,
				ReadOnly:       true,
			})
			if err != nil {
				panic(err)
			}
			defer stateDB.Close()
			stateStore := state.NewStore(stateDB, state.StoreOptions{
				DiscardABCIResponses: false,
			})
			blockStore := store.NewBlockStore(blockDB)

			fmt.Println("Loaded : ", dir+"/data/")
			fmt.Println("Input Start Height :", startHeight)
			fmt.Println("Input End Height :", endHeight)
			fmt.Println("Latest Height :", blockStore.Height())

			// checking start height
			block := blockStore.LoadBlock(startHeight)
			if block == nil {
				fmt.Println(startHeight, "is not available on this data")
				for i := 0; i < 1000000000000; i++ {
					block := blockStore.LoadBlock(int64(i))
					if block != nil {
						fmt.Println("available starting Height : ", i)
						break
					}
				}
				return nil
			}

			// checking end height
			if endHeight > blockStore.Height() {
				fmt.Println(endHeight, "is not available, Latest Height : ", blockStore.Height())
				return nil
			}
			//conn, err := sql.Open("mysql", "devops:passw0rd@tcp(51.195.63.75:3306)/crst_chart") // 1
			conn, err := sql.Open("mysql", DSN)
			if err != nil {
				fmt.Println(err)
			}
			defer conn.Close() // 3

			// store last_ts for each pairs
			type PairLastTs map[uint64]int64
			pairLastTs := PairLastTs{}
			cntInsert := 0 // number of inserted rows
			cntOrder := 0  // total Orders

			//Swap Data
			txReqMap := make(map[string]SwapReqRow, 8192)

			// main iteration
			for i := startHeight; i < endHeight; i++ {
				block := blockStore.LoadBlock(i)
				blockHeight := i
				blockTime := block.Time.UTC().Unix()

				results, err := stateStore.LoadABCIResponses(i)
				// https://pkg.go.dev/github.com/tendermint/tendermint@v0.34.22/proto/tendermint/state#ABCIResponses
				if err != nil {
					return err
				}

				for _, tx := range results.DeliverTxs {
					// find "order" related events
					for _, evt := range tx.Events {
						if strings.Contains(evt.Type, "order") {
							//fmt.Println(evt.Type)
						}
						pairStr := ""
						reqStr := ""

						if evt.Type == "limit_order" || evt.Type == "market_order" {
							req := SwapReqRow{}
							for _, att := range evt.Attributes {
								v := string(att.Value)
								switch string(att.Key) {
								case "orderer":
									req.Owner = v
								case "pair_id":
									pairStr = v
									pid, err := strconv.ParseUint(v, 10, 64)
									if err != nil {
										panic(err.Error())
									}
									req.PairId = pid
								case "order_id":
									reqStr = v
									rid, err := strconv.ParseUint(v, 10, 64)
									if err != nil {
										panic(err.Error())
									}
									req.ReqId = rid
								case "order_direction":
									if v == "ORDER_DIRECTION_BUY" {
										req.Direction = 1
									} else if v == "ORDER_DIRECTION_SELL" {
										req.Direction = 2
									} else {
										panic("no direction")
									}
								case "offer_coin":
									c, err := sdk.ParseCoinNormalized(v)
									if err != nil {
										panic("offer coin err")
									}
									req.OfferDenom = c.Denom
									req.OfferAmount = c.Amount.String()
								case "demand_coin_denom":
									req.DemandDenom = v
								case "price":
									req.Price = v //denom exp 적용 필요
								case "amount":
									req.OpenBaseAmount = v
								case "expire_at":
									t, err := time.Parse(time.RFC3339, v)
									if err != nil {
										fmt.Println(t)
										panic("time err")
									}
									req.ExpireTimestamp = t.Unix()
									//case "batch_id":

								} // end switch

								//req.TxHash =

							} // end attribute
							req.Status = 1
							req.Height = i

							kk := pairStr + "_" + reqStr
							txReqMap[kk] = req

							fmt.Print("[TX] ")
							fmt.Println(req)

						}
					}
				} // abciResponses.DeliverTxs iteration

				for _, event := range results.EndBlock.Events {
					if strings.Contains(event.String(), "order_result") {
						req := SwapReqRow{}
						pairStr := ""
						reqStr := ""
						for _, att := range event.Attributes {
							v := string(att.Value)
							switch string(att.Key) {
							case "orderer":
								req.Owner = v
							case "pair_id":
								pairStr = v
								pid, err := strconv.ParseUint(v, 10, 64)
								if err != nil {
									panic(err.Error())
								}
								req.PairId = pid
							case "order_id":
								reqStr = v
								rid, err := strconv.ParseUint(v, 10, 64)
								if err != nil {
									panic(err.Error())
								}
								req.ReqId = rid
							case "order_direction":
								if v == "ORDER_DIRECTION_BUY" {
									req.Direction = 1
								} else if v == "ORDER_DIRECTION_SELL" {
									req.Direction = 2
								} else {
									panic("no direction")
								}
							case "offer_coin":
								c, err := sdk.ParseCoinNormalized(v)
								if err != nil {
									panic("offer coin err")
								}
								req.OfferDenom = c.Denom
								req.OfferAmount = c.Amount.String()
							case "amount":
								req.FilledBaseAmount = v
							case "open_amount":
								req.OpenBaseAmount = v
							case "remaining_offer_coin":
								c, err := sdk.ParseCoinNormalized(v)
								if err != nil {
									panic("offer coin err")
								}
								req.RemainOfferAmount = c.Amount.String()
							case "received_coin":
								c, err := sdk.ParseCoinNormalized(v)
								if err != nil {
									panic("offer coin err")
								}
								req.DemandReceivedAmount = c.Amount.String()
							case "status":
								req.Status = int(liquiditytypes.OrderStatus_value[v])
								//case "batch_id":

							} // end switch
						}
						kk := pairStr + "_" + reqStr

						reqOrg, ok := txReqMap[kk]
						if ok {
							reqOrg.FilledBaseAmount = req.FilledBaseAmount
							reqOrg.OpenBaseAmount = req.OpenBaseAmount
							reqOrg.RemainOfferAmount = req.RemainOfferAmount
							reqOrg.DemandReceivedAmount = req.DemandReceivedAmount
							reqOrg.Status = req.Status
							reqOrg.UpdateHeight = i

							txReqMap[kk] = reqOrg
						} else {
							fmt.Println("skip. no req tx:" + kk)
							continue
						}

						//fmt.Println("chart 에 넣어야 할 트랜잭션 발견")
						//fmt.Println(txReqMap[kk])
						fmt.Println(i, "[endblock-o]", event.String())

						// start recover
						//원래는 블록타임 기준이 아닌 time.Now().UnixMicro() 값이 ts_60bar 기준이었음
						//ts_60bar := blockTime
						ts_60bar := int64(math.Floor(float64(blockTime)/60) * 60)
						ts_now := time.Now().Unix()
						lastTs := int64(0)
						tmpVolume, err := strconv.ParseFloat(reqOrg.FilledBaseAmount, 64)
						if err != nil {
							fmt.Println(err)
							continue
						}
						tmpPairId := reqOrg.PairId
						if err != nil {
							fmt.Println(err)
							continue
						}
						// check last_ts for a pair
						pairListTs, ok := pairLastTs[tmpPairId]
						if ok {
							lastTs = pairListTs
						}
						// if lastTs 와 같은 분 단위가 아니라면(1분 단위가 넘어갔다면) insert
						insertNew := (ts_60bar > lastTs || lastTs == 0)

						// DB insert & update
						func(insert bool, ts_now, ts_60bar, lastBarTs int64, pair uint64, price string, v float64) {
							//TODO: use connection pool instead (or seperated to other process)
							cntOrder++
							if insertNew {
								cntInsert++
								//new bar
								// chart_insert procedure 는 모든 bar 를 동시에 체크한다.
								// 장애 상황에서는 어떤 bar 는 이미 존재할 수 있다.
								// 존재하더라도 로직 상 무방하다. procedure 에서 이미 존재하는 것은 pass 하고 생성한다.
								call_insert := "CALL chart_insert(?,?,?,?,?)"
								_, err := conn.Exec(call_insert, pair, ts_60bar, price, price, lastBarTs)
								if err != nil {
									fmt.Println(err)
								} else {
									log.Println(blockHeight, "block, pairId", pair, "INSERTED")
								}
							}
							// update current bar
							// 장애 상황에서는 있어야 할 bar 가 없을 수도 있다고 생각할 수 있다.
							// 하지만 위 insert 문에서 모두 커버한다.
							// lastTs 가 없는 상황에서 프로그램이 시작되므로 bar 가 없다면 insert 후 시작한다.
							// 그 후 순차적으로 블록을 탐색하기 때문에 bar 는 반드시 존재한다.
							//updateQ := "UPDATE chart_data SET update_ts_sec = ?, c= ?, v = v + ?, cnt = cnt + 1, h = GREATEST(h,?), l = LEAST(l,?) WHERE uid IN (select * from (SELECT MAX(uid) as uid FROM chart_data WHERE pair_id=? GROUP BY resolution) AS X)"
							updateQ := "UPDATE chart_data SET update_ts_sec = ?, c= ?, v = v + ?, cnt = cnt + 1, h = GREATEST(h,?), l = LEAST(l,?) WHERE uid IN (select uid from chart_data where pair_id=? and ts_sec in (SELECT MAX(ts_sec) as ts_sec FROM chart_data WHERE ts_sec <= ? and pair_id=? GROUP BY resolution))"
							_, err = conn.Exec(updateQ, ts_now, price, v, price, price, pair, ts_60bar, pair)
							if err != nil {
								fmt.Println(err)
							} else {
								log.Println(blockHeight, "block, pairId", pair, "updated")
							}
						}(insertNew, ts_now, ts_60bar, lastTs, reqOrg.PairId, reqOrg.Price, tmpVolume)

						// update lastTs
						pairLastTs[tmpPairId] = ts_60bar

						// end recover
					} else if strings.Contains(event.String(), "user_order_matched") {
						// f := SwapFilledRow{}
						// dir := 0
						// var o, d sdk.Int

						// for _, att := range event.Attributes {
						// 	v := string(att.Value)
						// 	switch string(att.Key) {
						// 	case "orderer":
						// 		f.Owner = v
						// 	case "pair_id":
						// 		pid, err := strconv.ParseUint(v, 10, 64)
						// 		if err != nil {
						// 			panic(err.Error())
						// 		}
						// 		f.PairId = pid
						// 	case "order_id":
						// 		rid, err := strconv.ParseUint(v, 10, 64)
						// 		if err != nil {
						// 			panic(err.Error())
						// 		}
						// 		f.ReqId = rid
						// 	case "order_direction":
						// 		if v == "ORDER_DIRECTION_BUY" {
						// 			dir = 1
						// 		} else if v == "ORDER_DIRECTION_SELL" {
						// 			dir = 2
						// 		} else {
						// 			panic("no direction")
						// 		}
						// 	case "paid_coin":
						// 		c, err := sdk.ParseCoinNormalized(v)
						// 		if err != nil {
						// 			panic("offer coin err")
						// 		}
						// 		f.OfferDenom = c.Denom
						// 		f.FilledOfferAmount = c.Amount.String()
						// 		o = c.Amount

						// 	case "received_coin":
						// 		c, err := sdk.ParseCoinNormalized(v)
						// 		if err != nil {
						// 			panic("offer coin err")
						// 		}
						// 		f.DemandDenom = c.Denom
						// 		f.FilledDemandAmount = c.Amount.String()
						// 		d = c.Amount
						// 		//case "batch_id":

						// 	} // end switch

						// 	//f.Price
						// 	/*
						// 		f.Status
						// 		f.SwappedBaseAmount
						// 		f.Timestamp
						// 	*/
						// }
						// f.Height = i
						// if dir == 1 {
						// 	p := o.ToDec().QuoInt(d)
						// 	f.Price = p.String()
						// 	f.SwappedBaseAmount = d.String()
						// } else {
						// 	p := d.ToDec().QuoInt(o)
						// 	f.Price = p.String()
						// 	f.SwappedBaseAmount = o.String()
						// }
						// fmt.Println("user_order_matched 이벤트 발견")
						// fmt.Println(f)
						// fmt.Println(i, "[endblock-u]", event.String())

					}

				}
			} // iteration of startHeight ~ endHeight

			log.Println("total number of DB updated :", cntOrder)
			log.Println("total number of DB insert procedure called :", cntInsert)
			return nil
		}, // RunE end
	} // cmd end
	return cmd
}

func BlockParserCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:  "blockparser [chain-dir] [start-height] [end-height]",
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			// home directory which contains chain data. e.g. ~/.crescent
			dir := args[0]

			startHeight, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("parse start-Height: %w", err)
			}

			endHeight, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("parse end-Height: %w", err)
			}

			db, err := sdk.NewLevelDB("data/blockstore", dir)
			if err != nil {
				panic(err)
			}
			defer db.Close()

			stateDB, err := sdk.NewLevelDB("data/state", dir)
			if err != nil {
				panic(err)
			}
			defer stateDB.Close()

			blockStore := store.NewBlockStore(db)

			fmt.Println("Loaded : ", dir+"/data/")
			fmt.Println("Input Start Height :", startHeight)
			fmt.Println("Input End Height :", endHeight)
			fmt.Println("Latest Height :", blockStore.Height())

			// checking start height
			block := blockStore.LoadBlock(startHeight)
			if block == nil {
				fmt.Println(startHeight, "is not available on this data")
				for i := 0; i < 1000000000000; i++ {
					block := blockStore.LoadBlock(int64(i))
					if block != nil {
						fmt.Println("available starting Height : ", i)
						break
					}
				}
				return nil
			}

			// checking end height
			if endHeight > blockStore.Height() {
				fmt.Println(endHeight, "is not available, Latest Height : ", blockStore.Height())
				return nil
			}

			// Extract required data for LSV Performance scoring
			validatorMap := make(map[string]*ValidatorCommitInfo)
			emptyCommitMap := make(map[int]*EmptyCommit)
			proposerMap := make(map[int]*ProposerInfo)
			proposerTxMap := make(map[string]*ProposerTxInfo)

			// Gather data to create csv
			// 1) Proposer count per validator
			// 2) Proposer tx count per validator
			// 3) Commit count per validator
			for i := startHeight; i <= endHeight; i++ {

				// Extract proposer count and Txs data
				block := blockStore.LoadBlock(i)
				proposerInfo := ProposerInfo{
					Height:          i,
					ProposerAddress: fmt.Sprint(block.ProposerAddress),
					TxCount:         len(block.Txs),
				}
				proposerMap[int(i)] = &proposerInfo

				if _, ok := proposerTxMap[proposerInfo.ProposerAddress]; ok {
					proposerTxMap[proposerInfo.ProposerAddress].ProposingCount += 1
					proposerTxMap[proposerInfo.ProposerAddress].TxCount += proposerInfo.TxCount
				} else {
					proposerTxInfo := ProposerTxInfo{
						ProposerAddress: proposerInfo.ProposerAddress,
						ProposingCount:  1,
						TxCount:         proposerInfo.TxCount,
					}
					proposerTxMap[proposerInfo.ProposerAddress] = &proposerTxInfo
				}

				// Extract block commit data
				b, err := json.Marshal(blockStore.LoadBlockCommit(i))
				if err != nil {
					panic(err)
				}

				jsonString := string(b)
				var blockCommit = BlockCommit{}
				json.Unmarshal([]byte(jsonString), &blockCommit)

				// slot is the rank
				for slot, item := range blockCommit.Signatures {

					// if no signature in the slot,
					// this snippet can be removed - no use for now
					if item.ValidatorAddress == "" {

						_, ok := emptyCommitMap[slot]
						if !ok {
							emptyCommit := EmptyCommit{
								Slot: slot,
							}
							emptyCommitMap[slot] = &emptyCommit
						}

						emptyCommit := emptyCommitMap[slot]
						emptyCommit.Heights = append(emptyCommit.Heights, i)

						continue
					}

					_, ok := validatorMap[item.ValidatorAddress]
					if !ok {
						validatorCommitInfo := ValidatorCommitInfo{
							ValidatorAddress: item.ValidatorAddress,
							SlotCount:        1,
						}

						validatorCommitInfo.CommitInfos = append(validatorCommitInfo.CommitInfos, CommitInfo{
							Slot:        slot,
							StartHeight: i,
							EndHeight:   i,
							CommitCount: 1,
						})

						validatorMap[item.ValidatorAddress] = &validatorCommitInfo
					} else {
						validatorCommitInfo := validatorMap[item.ValidatorAddress]
						slotCount := validatorCommitInfo.SlotCount

						if slot == validatorCommitInfo.CommitInfos[slotCount-1].Slot {
							validatorCommitInfo.CommitInfos[slotCount-1].EndHeight = i
							validatorCommitInfo.CommitInfos[slotCount-1].CommitCount++
						} else {
							validatorCommitInfo.CommitInfos = append(validatorCommitInfo.CommitInfos, CommitInfo{
								Slot:        slot,
								StartHeight: i,
								EndHeight:   i,
								CommitCount: 1,
							})
							validatorCommitInfo.SlotCount++
						}
					}
				}
			}

			// 1) Proposer count per validator
			outputProposerFile, _ := os.OpenFile(fmt.Sprintf("proposer-%d-%d.csv", startHeight, endHeight), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			defer outputProposerFile.Close()

			writerProposer := csv.NewWriter(outputProposerFile)
			outputProposer := []string{
				"Height", "Proposer Address", "TX Count",
			}
			writeFile(writerProposer, outputProposer, outputProposerFile.Name())

			for _, p := range proposerMap {

				outputProposer := []string{
					fmt.Sprint(p.Height),
					fmt.Sprint(p.ProposerAddress),
					fmt.Sprint(p.TxCount),
				}

				writeFile(writerProposer, outputProposer, outputProposerFile.Name())
			}

			fmt.Println("Done! check the output files on current dir : ", outputProposerFile.Name())

			// 2) Proposer tx count per validator
			outputProposerTxFile, _ := os.OpenFile(fmt.Sprintf("proposer-tx-%d-%d.csv", startHeight, endHeight), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			defer outputProposerFile.Close()

			writerProposerTx := csv.NewWriter(outputProposerTxFile)
			outputProposerTx := []string{
				"Proposer Address", "Proposing Count", "TX Count",
			}
			writeFile(writerProposerTx, outputProposerTx, outputProposerTxFile.Name())

			for _, p := range proposerTxMap {

				outputProposerTx := []string{
					fmt.Sprint(p.ProposerAddress),
					fmt.Sprint(p.ProposingCount),
					fmt.Sprint(p.TxCount),
				}

				writeFile(writerProposerTx, outputProposerTx, outputProposerTxFile.Name())
			}

			fmt.Println("Done! check the output files on current dir : ", outputProposerTxFile.Name())

			// 3) Commit count per validator
			outputFile, _ := os.OpenFile(fmt.Sprintf("data-%d-%d.csv", startHeight, endHeight), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			defer outputFile.Close()

			writer := csv.NewWriter(outputFile)
			output := []string{
				"Validator Address", "Slot Count", "Slot", "Start Height", "End Height", "Commit Count", "Block Count", "Missed Commit",
			}
			writeFile(writer, output, outputFile.Name())

			for _, v := range validatorMap {

				for _, cv := range v.CommitInfos {

					blockCount := cv.EndHeight - cv.StartHeight + 1
					missedCommit := blockCount - cv.CommitCount

					output := []string{
						v.ValidatorAddress,
						fmt.Sprint(v.SlotCount),
						fmt.Sprint(cv.Slot),
						fmt.Sprint(cv.StartHeight),
						fmt.Sprint(cv.EndHeight),
						fmt.Sprint(cv.CommitCount),
						fmt.Sprint(blockCount),
						fmt.Sprint(missedCommit),
					}

					writeFile(writer, output, outputFile.Name())
				}
			}

			fmt.Println("Done! check the output files on current dir : ", outputFile.Name())
			return nil
		},
	}
	return cmd
}

// RPCParserCmd
// go run main.go https://rpc-juno-old-archive.cosmoapi.com 2017467 2578097
// Total 560,000 blocks
// Throughput 120,000 blocks/h
// Estimation 4.6 hours
func RPCParserCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "blockparser rpc [rpc url] [start-height] [end-height]",
		Args: cobra.ExactArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {

			startHeight, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("parse start-Height: %w", err)
			}

			endHeight, err := strconv.ParseInt(args[3], 10, 64)
			if err != nil {
				return fmt.Errorf("parse end-Height: %w", err)
			}

			rpcUrl := args[1]

			fmt.Println("RPC URL : ", rpcUrl)
			fmt.Println("Input Start Height :", startHeight)
			fmt.Println("Input End Height :", endHeight)

			// Gather data from RPC
			// - Commit count per validator
			for i := startHeight; i <= endHeight; i++ {
				// if i%10 == 0 {
				// 	t := time.Now()
				// 	fmt.Println(i, " - ", t)
				// }

				blockUrl := rpcUrl + "/block?height=" + strconv.FormatInt(i, 10)
				res, err := http.Get(blockUrl)
				if err != nil {
					panic(err)
				}

				body, err := ioutil.ReadAll(res.Body)
				if err != nil {
					panic(err)
				}
				jsonString := string(body)

				//codec
				cdc := app.MakeEncodingConfig().Marshaler
				var req liquiditytypes.Order

				rpcBlockData := RPCBlockData{}
				json.Unmarshal([]byte(jsonString), &rpcBlockData)

				txs := rpcBlockData.Result.Block.Data.Txs
				for _, tx := range txs {
					bz, _ := base64.StdEncoding.DecodeString(tx)
					//fmt.Println(tx)
					//fmt.Println(string(bz))

					if err := cdc.Unmarshal(bz, &req); err != nil {
						fmt.Println("Unmarshal err :", err)
					} else {
						fmt.Println("찾았다 시발")
					}
				}
			}

			outputFile, _ := os.OpenFile(fmt.Sprintf("data-%d-%d.csv", startHeight, endHeight), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			defer outputFile.Close()

			writer := csv.NewWriter(outputFile)
			output := []string{
				"Validator Address", "Slot Count", "Slot", "Start Height", "End Height", "Commit Count", "Block Count", "Missed Commit",
			}
			writeFile(writer, output, outputFile.Name())

			// for _, v := range validatorMap {

			// 	for _, cv := range v.CommitInfos {

			// 		blockCount := cv.EndHeight - cv.StartHeight + 1
			// 		missedCommit := blockCount - cv.CommitCount

			// 		output := []string{
			// 			v.ValidatorAddress,
			// 			fmt.Sprint(v.SlotCount),
			// 			fmt.Sprint(cv.Slot),
			// 			fmt.Sprint(cv.StartHeight),
			// 			fmt.Sprint(cv.EndHeight),
			// 			fmt.Sprint(cv.CommitCount),
			// 			fmt.Sprint(blockCount),
			// 			fmt.Sprint(missedCommit),
			// 		}

			// 		writeFile(writer, output, outputFile.Name())
			// 	}
			// }

			fmt.Println("Done! check the output files on current dir : ", outputFile.Name())
			return nil
		},
	}
	return cmd
}

func ConsensusParserCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "blockparser consensus [rpc url]",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {

			rpcUrl := args[1]

			fmt.Println("RPC URL : ", rpcUrl)

			res, err := http.Get(rpcUrl)
			if err != nil {
				panic(err)
			}

			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				panic(err)
			}

			jsonString := string(body)

			consensusStateData := ConsensusStateInfo{}
			json.Unmarshal([]byte(jsonString), &consensusStateData)

			var lastesIndex int = len(consensusStateData.Result.RoundState.HeightVoteSet) - 2
			fmt.Println(consensusStateData.Result.RoundState.HeightVoteSet[lastesIndex].Round)

			prevoteMap := make(map[string]int)

			for _, item := range consensusStateData.Result.RoundState.HeightVoteSet[lastesIndex].Prevotes {

				var key string = ""
				if item == "nil-Vote" {
					key = "nil-Vote"

				} else {
					s := strings.Split(item, " ")
					key = s[2]
				}

				fmt.Println(key)
				_, ok := prevoteMap[key]
				if !ok {
					prevoteMap[key] = 1
				} else {
					prevoteMap[key] += 1
				}
			}

			for key, value := range prevoteMap {
				fmt.Println(key, value)
			}

			return nil
		},
	}
	return cmd
}

func writeFile(w *csv.Writer, result []string, fileName string) {
	if err := w.Write(result); err != nil {
		return
	}
	w.Flush()

	if err := w.Error(); err != nil {
		return
	}
}

func NewBlockParserCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "blockparser [chain-name] [chain-dir] [start-height] [end-height]",
		Args: cobra.ExactArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {
			chain := args[0]
			dir := args[1]
			startHeight, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("parse start-Height: %w", err)
			}

			endHeight, err := strconv.ParseInt(args[3], 10, 64)
			if err != nil {
				return fmt.Errorf("parse end-Height: %w", err)
			}

			blockDB, err := tmdb.NewGoLevelDBWithOpts("data/blockstore", dir, &opt.Options{
				ErrorIfMissing: true,
				ReadOnly:       true,
			})

			if err != nil {
				panic(err)
			}
			defer blockDB.Close()

			stateDB, err := tmdb.NewGoLevelDBWithOpts("data/state", dir, &opt.Options{
				ErrorIfMissing: true,
				ReadOnly:       true,
			})

			if err != nil {
				panic(err)
			}
			defer stateDB.Close()
			stateStore := state.NewStore(stateDB, state.StoreOptions{
				DiscardABCIResponses: false,
			})

			blockStore := store.NewBlockStore(blockDB)

			fmt.Println("Loaded : ", dir+"/data/")
			fmt.Println("Input Start Height :", startHeight)
			fmt.Println("Input End Height :", endHeight)
			fmt.Println("Latest Height :", blockStore.Height())

			// checking start height
			block := blockStore.LoadBlock(startHeight)
			if block == nil {
				fmt.Println(startHeight, "is not available on this data")
				for i := 0; i < 1000000000000; i++ {
					block := blockStore.LoadBlock(int64(i))
					if block != nil {
						fmt.Println("available starting Height : ", i)
						break
					}
				}
				return nil
			}

			// checking end height
			if endHeight > blockStore.Height() {
				fmt.Println(endHeight, "is not available, Latest Height : ", blockStore.Height())
				return nil
			}

			conn, err := sql.Open("mysql", "devops:passw0rd@tcp(127.0.0.1:3306)/backend") // 1
			if err != nil {
				fmt.Println(err)
			}
			defer conn.Close() // 3

			fmt.Printf("DB 연동: %+v\n", conn.Stats()) // 2

			ibcEventTypes := map[string]bool{
				"send_packet": true,
				// "ibc_transfer":        true,

				"recv_packet": true,
				// "write_acknowledgement" : true,
				// "denomination_trace" : true,
				// "fungible_token_packet" : true,

				"acknowledge_packet": true,
				// "fungible_token_packet" : true,

				"timeout_packet": true,
				// "timeout": true,
			}

			fmt.Println(len(ibcEventTypes))

			// packets := []EventPacket{}

			for i := startHeight; i < endHeight; i++ {

				block := blockStore.LoadBlock(i)
				blockHeight := i
				blockTime := block.Time.UTC().Unix()

				results, err := stateStore.LoadABCIResponses(i)
				// https://pkg.go.dev/github.com/tendermint/tendermint@v0.34.22/proto/tendermint/state#ABCIResponses

				if err != nil {
					return err
				}

				for _, tx := range results.DeliverTxs {

					var p = EventPacket{}
					p.block_height = strconv.FormatInt(blockHeight, 10)
					p.block_time = strconv.FormatInt(blockTime, 10)

					for _, evt := range tx.Events {
						// https://pkg.go.dev/github.com/tendermint/tendermint@v0.34.22/abci/types#Event

						if ibcEventTypes[evt.Type] {

							p.event_type = evt.Type
							for _, attr := range evt.Attributes {
								// https://pkg.go.dev/github.com/tendermint/tendermint@v0.34.22/abci/types#EventAttribute

								// fmt.Println(BytesToString(attr.Key), BytesToString(attr.Value), attr.Index)
								key := string(attr.Key)
								value := string(attr.Value)

								switch key {
								case "packet_timeout_height":
									p.packet_timeout_height = value
								case "packet_timeout_timestamp":
									p.packet_timeout_timestamp = value
								case "packet_sequence":
									p.packet_sequence = value
								case "packet_src_port":
									p.packet_src_port = value
								case "packet_src_channel":
									p.packet_src_channel = value
								case "packet_dst_port":
									p.packet_dst_port = value
								case "packet_dst_channel":
									p.packet_dst_channel = value
								case "packet_channel_ordering":
									p.packet_channel_ordering = value
								case "packet_connection":
									p.packet_connection = value
								}

							}

							insertStr := fmt.Sprintf("insert ignore into packets (chain, block_height, block_time, event_type, packet_timeout_height, packet_timeout_timestamp, packet_sequence, packet_src_port, packet_src_channel, packet_dst_port, packet_dst_channel, packet_channel_ordering, packet_connection) value ('%s', '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')", chain, p.block_height, p.block_time, p.event_type, p.packet_timeout_height, p.packet_timeout_timestamp, p.packet_sequence, p.packet_src_port, p.packet_src_channel, p.packet_dst_port, p.packet_dst_channel, p.packet_channel_ordering, p.packet_connection)

							_, err := conn.Exec(insertStr)
							if err != nil {
								return err
							}

							// return nil
						}
					}

				}
			}

			return nil
		},
	}
	return cmd
}

func BlockParserCmdForChartBk() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "blockparser chart [chain-dir] [start-height] [end-height]",
		Args: cobra.ExactArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := args[1]
			startHeight, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("parse start-Height: %w", err)
			}
			endHeight, err := strconv.ParseInt(args[3], 10, 64)
			if err != nil {
				return fmt.Errorf("parse end-Height: %w", err)
			}

			blockDB, err := tmdb.NewGoLevelDBWithOpts(BLOCK_DB, dir, &opt.Options{
				ErrorIfMissing: true,
				ReadOnly:       true,
			})
			if err != nil {
				panic(err)
			}
			defer blockDB.Close()

			stateDB, err := tmdb.NewGoLevelDBWithOpts(STATE_DB, dir, &opt.Options{
				ErrorIfMissing: true,
				ReadOnly:       true,
			})
			if err != nil {
				panic(err)
			}
			defer stateDB.Close()
			stateStore := state.NewStore(stateDB, state.StoreOptions{
				DiscardABCIResponses: false,
			})
			blockStore := store.NewBlockStore(blockDB)

			fmt.Println("Loaded : ", dir+"/data/")
			fmt.Println("Input Start Height :", startHeight)
			fmt.Println("Input End Height :", endHeight)
			fmt.Println("Latest Height :", blockStore.Height())

			// checking start height
			block := blockStore.LoadBlock(startHeight)
			if block == nil {
				fmt.Println(startHeight, "is not available on this data")
				for i := 0; i < 1000000000000; i++ {
					block := blockStore.LoadBlock(int64(i))
					if block != nil {
						fmt.Println("available starting Height : ", i)
						break
					}
				}
				return nil
			}

			// checking end height
			if endHeight > blockStore.Height() {
				fmt.Println(endHeight, "is not available, Latest Height : ", blockStore.Height())
				return nil
			}
			//conn, err := sql.Open("mysql", "devops:passw0rd@tcp(51.195.63.75:3306)/crst_chart") // 1
			conn, err := sql.Open("mysql", DSN)
			if err != nil {
				fmt.Println(err)
			}
			defer conn.Close() // 3

			// store last_ts for each pairs
			type PairLastTs map[uint64]int64
			pairLastTs := PairLastTs{}
			cntInsert := 0 // number of inserted rows
			cntOrder := 0  // total Orders

			// main iteration
			for i := startHeight; i < endHeight; i++ {
				block := blockStore.LoadBlock(i)
				blockHeight := i
				blockTime := block.Time.UTC().Unix()

				abciResponses, err := stateStore.LoadABCIResponses(i)
				// https://pkg.go.dev/github.com/tendermint/tendermint@v0.34.22/proto/tendermint/state#ABCIResponses
				if err != nil {
					return err
				}

				for _, tx := range abciResponses.DeliverTxs {
					//fmt.Println("tx.Data : ", string(tx.Data))
					//fmt.Println("tx.Events length : ", len(tx.Events))
					//fmt.Println(tx.Info)
					//fmt.Println(tx.Log)
					//fmt.Println(tx.XXX_Unmarshal(tx.GetData()))

					// find "order" related events
					for _, evt := range tx.Events {
						// https://pkg.go.dev/github.com/tendermint/tendermint@v0.34.22/abci/types#Event
						if strings.Contains(evt.Type, "order") {
							if evt.Type == "market_order" || evt.Type == "limit_order" {
								var oe = OrderEvent{}
								oe.BlockHeight = strconv.FormatInt(blockHeight, 10)
								oe.BlockTime = blockTime
								oe.EventType = evt.Type

								for _, attr := range evt.Attributes {
									// https://pkg.go.dev/github.com/tendermint/tendermint@v0.34.22/abci/types#EventAttribute
									key := string(attr.Key)
									value := string(attr.Value)

									switch key {
									case "orderer":
										oe.Orderer = value
									case "pair_id":
										oe.PairId = value
									case "order_direction":
										oe.OrderDirection = value
									case "offer_coin":
										oe.OfferCoin = value
									case "demand_coin_denom":
										oe.DemandCoinDenom = value
									case "price":
										oe.Price = value
									case "amount":
										oe.Amount = value
									case "order_id":
										oe.OrderId = value
									case "batch_id":
										oe.BatchId = value
									case "expire_at":
										oe.ExpireAt = value
									case "refunded_coins":
										oe.RefundedCoins = value
									}
								}

								//원래는 블록타임 기준이 아닌 time.Now().UnixMicro() 값이 ts_60bar 기준이었음
								//ts_60bar := blockTime
								ts_60bar := int64(math.Floor(float64(blockTime)/60) * 60)
								ts_now := time.Now().Unix()
								lastTs := int64(0)
								tmpVolume, err := strconv.ParseFloat(oe.Amount, 64)
								if err != nil {
									fmt.Println(err)
									continue
								}
								tmpPairId, err := strconv.ParseUint(oe.PairId, 10, 64)
								if err != nil {
									fmt.Println(err)
									continue
								}
								// check last_ts for a pair
								pairListTs, ok := pairLastTs[tmpPairId]
								if ok {
									lastTs = pairListTs
								}
								// if lastTs 와 같은 분 단위가 아니라면(1분 단위가 넘어갔다면) insert
								insertNew := (ts_60bar > lastTs || lastTs == 0)

								// DB insert & update
								func(insert bool, ts_now, ts_60bar, lastBarTs int64, pair string, price string, v float64) {
									//TODO: use connection pool instead (or seperated to other process)
									cntOrder++
									if insertNew {
										cntInsert++
										//new bar
										// chart_insert procedure 는 모든 bar 를 동시에 체크한다.
										// 장애 상황에서는 어떤 bar 는 이미 존재할 수 있다.
										// 존재하더라도 로직 상 무방하다. procedure 에서 이미 존재하는 것은 pass 하고 생성한다.
										call_insert := "CALL chart_insert(?,?,?,?,?)"
										_, err := conn.Exec(call_insert, pair, ts_60bar, price, price, lastBarTs)
										if err != nil {
											fmt.Println(err)
										} else {
											log.Println(blockHeight, "block, pairId", pair, "INSERTED")
										}
									}
									// update current bar
									// 장애 상황에서는 있어야 할 bar 가 없을 수도 있다고 생각할 수 있다.
									// 하지만 위 insert 문에서 모두 커버한다.
									// lastTs 가 없는 상황에서 프로그램이 시작되므로 bar 가 없다면 insert 후 시작한다.
									// 그 후 순차적으로 블록을 탐색하기 때문에 bar 는 반드시 존재한다.
									//updateQ := "UPDATE chart_data SET update_ts_sec = ?, c= ?, v = v + ?, cnt = cnt + 1, h = GREATEST(h,?), l = LEAST(l,?) WHERE uid IN (select * from (SELECT MAX(uid) as uid FROM chart_data WHERE pair_id=? GROUP BY resolution) AS X)"
									updateQ := "UPDATE chart_data SET update_ts_sec = ?, c= ?, v = v + ?, cnt = cnt + 1, h = GREATEST(h,?), l = LEAST(l,?) WHERE uid IN (select uid from chart_data where pair_id=? and ts_sec in (SELECT MAX(ts_sec) as ts_sec FROM chart_data WHERE ts_sec <= ? and pair_id=? GROUP BY resolution))"
									_, err = conn.Exec(updateQ, ts_now, price, v, price, price, pair, ts_60bar, pair)
									if err != nil {
										fmt.Println(err)
									} else {
										log.Println(blockHeight, "block, pairId", pair, "updated")
									}
								}(insertNew, ts_now, ts_60bar, lastTs, oe.PairId, oe.Price, tmpVolume)

								//new bar
								// call_insert := "CALL chart_insert(?,?,?,?,?)"
								// _, err = conn.Exec(call_insert, oe.pairId, ts_60bar, oe.price, oe.price, lastTs)
								// if err != nil {
								// 	fmt.Println(err)
								// } else {
								// 	log.Println(blockHeight)
								// }

								// update lastTs
								pairLastTs[tmpPairId] = ts_60bar
							}
						}
					} // event type end
				} // abciResponses.DeliverTxs iteration
			} // iteration of startHeight ~ endHeight

			log.Println("total number of DB updated :", cntOrder)
			log.Println("total number of DB insert procedure called :", cntInsert)
			return nil
		}, // RunE end
	} // cmd end
	return cmd
}
