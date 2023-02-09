package cmd

import (
	"time"
)

type EventPacket struct {
	block_height             string
	block_time               string
	event_type               string
	packet_timeout_height    string
	packet_timeout_timestamp string
	packet_sequence          string
	packet_src_port          string
	packet_src_channel       string
	packet_dst_port          string
	packet_dst_channel       string
	packet_channel_ordering  string
	packet_connection        string
}

type PacketData struct {
	block_height       uint
	block_time         time.Time
	event_type         string
	packet_sequence    string
	packet_src_channel string
	packet_dst_channel string
	amount             uint
	denom              string
	receiver           string
	sender             string
}

type TimeoutData struct {
	block_height       uint
	block_time         time.Time
	event_type         string
	packet_sequence    string
	packet_src_channel string
	packet_dst_channel string
	module             string
	refund_receiver    string
	refund_denom       string
	refund_amount      uint
	memo               string
}

type ConsensusStateInfo struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  struct {
		RoundState struct {
			HeightRoundStep   string    `json:"height/round/step"`
			StartTime         time.Time `json:"start_time"`
			ProposalBlockHash string    `json:"proposal_block_hash"`
			LockedBlockHash   string    `json:"locked_block_hash"`
			ValidBlockHash    string    `json:"valid_block_hash"`
			HeightVoteSet     []struct {
				Round              int      `json:"round"`
				Prevotes           []string `json:"prevotes"`
				PrevotesBitArray   string   `json:"prevotes_bit_array"`
				Precommits         []string `json:"precommits"`
				PrecommitsBitArray string   `json:"precommits_bit_array"`
			} `json:"height_vote_set"`
			Proposer struct {
				Address string `json:"address"`
				Index   int    `json:"index"`
			} `json:"proposer"`
		} `json:"round_state"`
	} `json:"result"`
}

type BlockCommit struct {
	Height  int `json:"height"`
	Round   int `json:"round"`
	BlockID struct {
		Hash  string `json:"hash"`
		Parts struct {
			Total int    `json:"total"`
			Hash  string `json:"hash"`
		} `json:"parts"`
	} `json:"block_id"`
	Signatures []struct {
		BlockIDFlag      int       `json:"block_id_flag"`
		ValidatorAddress string    `json:"validator_address"`
		Timestamp        time.Time `json:"timestamp"`
		Signature        string    `json:"signature"`
	} `json:"signatures"`
}

type ValidatorCommitInfo struct {
	ValidatorAddress string       `json:"validator_address"`
	SlotCount        int          `json:"slot_count"`
	CommitInfos      []CommitInfo `json:"commit_infos"`
}

type ProposerInfo struct {
	Height          int64  `json:"height"`
	ProposerAddress string `json:"proposer_address"`
	TxCount         int    `json:"tx_count"`
}

type ProposerTxInfo struct {
	ProposerAddress string `json:"proposer_address"`
	ProposingCount  int    `json:"proposer_count"`
	TxCount         int    `json:"tx_count"`
}

type CommitInfo struct {
	Slot        int   `json:"slot"`
	StartHeight int64 `json:"start_height"`
	EndHeight   int64 `json:"end_height"`
	CommitCount int64 `json:"commit_count"`
}

type EmptyCommit struct {
	Slot    int     `json:"slot"`
	Heights []int64 `json:"height"`
}

type RPCBlockData struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  struct {
		BlockID struct {
			Hash  string `json:"hash"`
			Parts struct {
				Total int    `json:"total"`
				Hash  string `json:"hash"`
			} `json:"parts"`
		} `json:"block_id"`
		Block struct {
			Header struct {
				Version struct {
					Block string `json:"block"`
				} `json:"version"`
				ChainID     string    `json:"chain_id"`
				Height      string    `json:"height"`
				Time        time.Time `json:"time"`
				LastBlockID struct {
					Hash  string `json:"hash"`
					Parts struct {
						Total int    `json:"total"`
						Hash  string `json:"hash"`
					} `json:"parts"`
				} `json:"last_block_id"`
				LastCommitHash     string `json:"last_commit_hash"`
				DataHash           string `json:"data_hash"`
				ValidatorsHash     string `json:"validators_hash"`
				NextValidatorsHash string `json:"next_validators_hash"`
				ConsensusHash      string `json:"consensus_hash"`
				AppHash            string `json:"app_hash"`
				LastResultsHash    string `json:"last_results_hash"`
				EvidenceHash       string `json:"evidence_hash"`
				ProposerAddress    string `json:"proposer_address"`
			} `json:"header"`
			Data struct {
				Txs []string `json:"txs"`
			} `json:"data"`
			Evidence struct {
				Evidence []interface{} `json:"evidence"`
			} `json:"evidence"`
			LastCommit struct {
				Height  string `json:"height"`
				Round   int    `json:"round"`
				BlockID struct {
					Hash  string `json:"hash"`
					Parts struct {
						Total int    `json:"total"`
						Hash  string `json:"hash"`
					} `json:"parts"`
				} `json:"block_id"`
				Signatures []struct {
					BlockIDFlag      int       `json:"block_id_flag"`
					ValidatorAddress string    `json:"validator_address"`
					Timestamp        time.Time `json:"timestamp"`
					Signature        string    `json:"signature"`
				} `json:"signatures"`
			} `json:"last_commit"`
		} `json:"block"`
	} `json:"result"`
}

// type OrderEvent struct {
// 	block_height      string //block_info
// 	block_time        string //block_info
// 	event_type        string //block_info
// 	orderer           string
// 	pair_id           string
// 	order_direction   string
// 	offer_coin        string
// 	demand_coin_denom string
// 	price             string
// 	amount            string
// 	order_id          string
// 	batch_id          string
// 	expire_at         string
// 	refunded_coins    string
// }

type OrderEvent struct {
	BlockHeight     string //block_info
	BlockTime       int64  //block_info
	EventType       string //block_info
	Orderer         string
	PairId          string
	OrderDirection  string
	OfferCoin       string
	DemandCoinDenom string
	Price           string
	Amount          string
	OrderId         string
	BatchId         string
	ExpireAt        string
	RefundedCoins   string
}
