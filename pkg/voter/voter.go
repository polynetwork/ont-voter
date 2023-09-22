/**
 * Copyright (C) 2021 The poly network Authors
 * This file is part of The poly network library.
 *
 * The poly network is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The poly network is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with the poly network.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package voter

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts/abi"
	ontology_go_sdk "github.com/ontio/ontology-go-sdk"
	"github.com/polynetwork/poly/core/types"
	"math/big"
	"strings"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/polynetwork/eth-contracts/go_abi/eccm_abi"
	sdk "github.com/polynetwork/poly-go-sdk"
	"github.com/polynetwork/poly/common"
	common2 "github.com/polynetwork/poly/native/service/cross_chain_manager/common"
	autils "github.com/polynetwork/poly/native/service/utils"
	"github.com/polynetwork/side-voter/config"
	"github.com/polynetwork/side-voter/pkg/db"
	"github.com/polynetwork/side-voter/pkg/log"
)

type Voter struct {
	polySdk *sdk.PolySdk
	signer  *sdk.Account
	conf    *config.Config
	clients []*ontology_go_sdk.OntologySdk
	bdb     *db.BoltDB
	//contracts         []*eccm_abi.EthCrossChainManager
	contractAddr      ethcommon.Address
	crossChainTopicID ethcommon.Hash
	crossChainEvent   string
	ccmAbiParsed      abi.ABI
	idx               int
}

func New(polySdk *sdk.PolySdk, signer *sdk.Account, conf *config.Config) *Voter {
	return &Voter{polySdk: polySdk, signer: signer, conf: conf}
}

func (v *Voter) Init() (err error) {
	var clients []*ontology_go_sdk.OntologySdk
	for _, node := range v.conf.SideConfig.RestURL {
		client := ontology_go_sdk.NewOntologySdk()
		client.NewRpcClient().SetAddress(node)
		_, err = client.GetCurrentBlockHeight()
		if err != nil {
			log.Fatalf("GetCurrentBlockHeight failed:%v", err)
		}

		clients = append(clients, client)
	}
	v.clients = clients

	bdb, err := db.NewBoltDB(v.conf.BoltDbPath)
	if err != nil {
		return
	}

	v.bdb = bdb

	v.contractAddr = ethcommon.HexToAddress(v.conf.SideConfig.ECCMContractAddress)
	ethCrossChainManagerAbiParsed, err := abi.JSON(strings.NewReader(eccm_abi.EthCrossChainManagerABI))
	if err != nil {
		return
	}
	v.ccmAbiParsed = ethCrossChainManagerAbiParsed
	if event, ok := ethCrossChainManagerAbiParsed.Events["CrossChainEvent"]; !ok {
		return fmt.Errorf("CrossChainEvent no tin ethCrossChainManagerAbiParsed.Events")
	} else {
		v.crossChainTopicID = event.ID
	}
	v.crossChainEvent = "CrossChainEvent"

	return
}

func (v *Voter) StartReplenish(ctx context.Context) {
	var nextPolyHeight uint64
	if v.conf.ForceConfig.PolyHeight != 0 {
		nextPolyHeight = v.conf.ForceConfig.PolyHeight
	} else {
		h, err := v.polySdk.GetCurrentBlockHeight()
		if err != nil {
			panic(fmt.Sprintf("v.polySdk.GetCurrentBlockHeight failed:%v", err))
		}
		nextPolyHeight = uint64(h)
		log.Infof("start from current poly height:%d", h)
	}
	ticker := time.NewTicker(time.Second * 2)
	for {
		select {
		case <-ticker.C:
			h, err := v.polySdk.GetCurrentBlockHeight()
			if err != nil {
				log.Errorf("v.polySdk.GetCurrentBlockHeight failed:%v", err)
				continue
			}
			height := uint64(h)
			log.Infof("current poly height:%d", height)
			if height < nextPolyHeight {
				continue
			}

			for nextPolyHeight <= height {
				select {
				case <-ctx.Done():
					return
				default:
				}
				log.Infof("handling poly height:%d", nextPolyHeight)
				events, err := v.polySdk.GetSmartContractEventByBlock(uint32(nextPolyHeight))
				if err != nil {
					log.Errorf("poly failed to fetch smart contract events for height %d, err %v", height, err)
					continue
				}
				txHashList := make([]interface{}, 0)
				for _, event := range events {
					for _, notify := range event.Notify {
						if notify.ContractAddress != autils.ReplenishContractAddress.ToHexString() {
							continue
						}
						states, ok := notify.States.([]interface{})
						if !ok || states[0].(string) != "ReplenishTx" {
							continue
						}

						chainId := states[2].(float64)
						if uint64(chainId) == v.conf.SideConfig.SideChainId {
							txHashes := states[1].([]interface{})
							txHashList = append(txHashList, txHashes...)
						}
					}
				}

				for _, txHash := range txHashList {
					err = v.fetchLockDepositEventByTxHash(txHash.(string))
					if err != nil {
						log.Errorf("fetchLockDepositEventByTxHash failed:%v", err)
						//change endpoint and retry, mutex is not used
						v.changeEndpoint()
						continue
					}
				}
				nextPolyHeight++
			}
		}
	}
}

func (v *Voter) StartVoter(ctx context.Context) {
	nextSideHeight := v.bdb.GetSideHeight()
	if v.conf.ForceConfig.SideHeight > 0 {
		nextSideHeight = v.conf.ForceConfig.SideHeight
	}
	var batchLength uint64 = 1
	if v.conf.SideConfig.Batch > 0 {
		batchLength = v.conf.SideConfig.Batch
	}
	ticker := time.NewTicker(time.Second * 2)
	for {
		select {
		case <-ticker.C:
			height, err := ontevmGetCurrentHeight(v.clients[v.idx])
			if err != nil {
				log.Errorf("ontevmGetCurrentHeight failed:%v", err)
				v.changeEndpoint()
				continue
			}
			log.Infof("current side height:%d", height)
			if height < nextSideHeight+v.conf.SideConfig.BlocksToWait+1 {
				continue
			}

			if v.conf.SideConfig.TimeToWait > 0 {
				time.Sleep(time.Second * time.Duration(v.conf.SideConfig.TimeToWait))
			}
			for nextSideHeight < height-v.conf.SideConfig.BlocksToWait-1 {
				select {
				case <-ctx.Done():
					v.bdb.Close()
					return
				default:
				}
				log.Infof("handling side height:%d", nextSideHeight)
				endSideHeight := nextSideHeight + batchLength - 1
				if endSideHeight > height-v.conf.SideConfig.BlocksToWait-1 {
					endSideHeight = height - v.conf.SideConfig.BlocksToWait - 1
				}
				err := v.fetchLockDepositEvents(nextSideHeight)
				if err != nil {
					log.Errorf("fetchLockDepositEvents failed:%v", err)
					v.changeEndpoint()
					sleep()
					continue
				}
				nextSideHeight += 1
			}

			err = v.bdb.UpdateSideHeight(nextSideHeight)
			if err != nil {
				log.Errorf("UpdateSideHeight failed:%v", err)
			}

		case <-ctx.Done():
			v.bdb.Close()
			log.Info("quiting from signal...")
			return
		}
	}
}

type CrossTransfer struct {
	txIndex string
	txId    []byte
	value   []byte
	toChain uint32
	height  uint64
}

func (v *Voter) fetchLockDepositEventByTxHash(ethTxHash string) error {
	client := v.clients[v.idx]
	//hash := ethcommon.HexToHash(txHash)
	smartContactEvent, err := client.GetSmartContractEvent(ethTxHash)
	if err != nil {
		return fmt.Errorf("GetSmartContractEvent err: %v", err.Error())
	}
	height, err := client.GetBlockHeightByTxHash(ethTxHash)
	if err != nil {
		return fmt.Errorf("GetBlockHeightByTxHash err: %v", err.Error())
	}
	latestHeight, err := ontevmGetCurrentHeight(v.clients[v.idx])
	if err != nil {
		return err
	}
	if uint64(height)+v.conf.SideConfig.BlocksToWait > latestHeight {
		return fmt.Errorf("transaction is not confirmed yet %s", ethTxHash)
	}

	for _, notify := range smartContactEvent.Notify {
		if strings.EqualFold(HexStringReverse(notify.ContractAddress), strings.TrimPrefix(v.contractAddr.Hex(), "0x")) {
			storageLog, err := deserializeStorageLog(notify)
			if err != nil {
				continue
			}
			for _, topic := range storageLog.Topics {
				switch topic {
				case v.crossChainTopicID:
					log.Info("(ccm CrossChain) height: %d, txhash: %s", height, ethTxHash)
					var evt eccm_abi.EthCrossChainManagerCrossChainEvent
					err = v.ccmAbiParsed.Unpack(&evt, "CrossChainEvent", storageLog.Data)
					if err != nil {
						continue
					}
					param := &common2.MakeTxParam{}
					_ = param.Deserialization(common.NewZeroCopySource([]byte(evt.Rawdata)))
					if !v.conf.IsWhitelistMethod(param.Method) {
						log.Errorf("target contract method invalid %s, height: %d, ethTxHash: %v", param.Method, height, ethTxHash)
						continue
					}

					raw, _ := v.polySdk.GetStorage(autils.CrossChainManagerContractAddress.ToHexString(),
						append(append([]byte(common2.DONE_TX), autils.GetUint64Bytes(v.conf.SideConfig.SideChainId)...), param.CrossChainID...))
					if len(raw) != 0 {
						log.Infof("fetchLockDepositEventByTxHash - ccid %s (tx_hash: %s) height: %v already on poly",
							hex.EncodeToString(param.CrossChainID), ethTxHash, height)
						continue
					}

					index := big.NewInt(0)
					index.SetBytes(evt.TxId)
					crossTx := &CrossTransfer{
						txIndex: encodeBigInt(index),
						txId:    ethcommon.Hex2Bytes(ethTxHash),
						toChain: uint32(evt.ToChainId),
						value:   []byte(evt.Rawdata),
						height:  uint64(height),
					}

					txHash, err := v.commitVote(uint32(height), crossTx.value, crossTx.txId)
					if err != nil {
						log.Errorf("commitVote failed:%v, height: %d, ethTxHash: %v", err, height, txHash)
						continue
					}
				}
			}
		}
	}
	return nil
}

func (v *Voter) commitVote(height uint32, value []byte, txhash []byte) (string, error) {
	log.Infof("commitVote, height: %d, value: %s, txhash: %s", height, hex.EncodeToString(value), hex.EncodeToString(txhash))
	tx, err := v.polySdk.Native.Ccm.ImportOuterTransfer(
		v.conf.SideConfig.SideChainId,
		value,
		height,
		nil,
		v.signer.Address[:],
		[]byte{},
		v.signer)
	if err != nil {
		return "", err
	} else {
		log.Infof("commitVote - send transaction to poly chain: ( poly_txhash: %s, eth_txhash: %s, height: %d )",
			tx.ToHexString(), ethcommon.BytesToHash(txhash).String(), height)
		return tx.ToHexString(), nil
	}
}

func (v *Voter) waitTx(txHash string) (err error) {
	start := time.Now()
	var tx *types.Transaction
	for {
		tx, err = v.polySdk.GetTransaction(txHash)
		if tx == nil || err != nil {
			if time.Since(start) > time.Minute*5 {
				err = fmt.Errorf("waitTx timeout")
				return
			}
			time.Sleep(time.Second)
			continue
		}
		return
	}
}

//change endpoint and retry, mutex is not used
func (v *Voter) changeEndpoint() {
	if v.idx == len(v.clients)-1 {
		v.idx = 0
	} else {
		v.idx = v.idx + 1
	}
	log.Infof("change endpoint to %d", v.idx)
}

func sleep() {
	time.Sleep(time.Second)
}

func (v *Voter) fetchLockDepositEvents(height uint64) error {
	log.Infof("fetchLockDepositEvents......  height: %v", height)
	events, err := v.clients[v.idx].GetSmartContractEventByBlock(uint32(height))
	if err != nil {
		log.Errorf("fetchLockDepositEvents GetSmartContractEventByBlock err: %v", err)
	}
	for _, event := range events {
		ethTxHash := HexStringReverse(event.TxHash)
		for _, notify := range event.Notify {
			if strings.EqualFold(HexStringReverse(notify.ContractAddress), strings.TrimPrefix(v.contractAddr.Hex(), "0x")) {
				storageLog, err := deserializeStorageLog(notify)
				if err != nil {
					continue
				}
				for _, topic := range storageLog.Topics {
					switch topic {
					case v.crossChainTopicID:
						log.Info("(ccm CrossChain) height: %d, txhash: %s", height, ethTxHash)
						var evt eccm_abi.EthCrossChainManagerCrossChainEvent
						err = v.ccmAbiParsed.Unpack(&evt, "CrossChainEvent", storageLog.Data)
						if err != nil {
							continue
						}
						param := &common2.MakeTxParam{}
						_ = param.Deserialization(common.NewZeroCopySource([]byte(evt.Rawdata)))
						if !v.conf.IsWhitelistMethod(param.Method) {
							log.Errorf("target contract method invalid %s, height: %d, ethTxHash: %v", param.Method, height, ethTxHash)
							continue
						}

						raw, _ := v.polySdk.GetStorage(autils.CrossChainManagerContractAddress.ToHexString(),
							append(append([]byte(common2.DONE_TX), autils.GetUint64Bytes(v.conf.SideConfig.SideChainId)...), param.CrossChainID...))
						if len(raw) != 0 {
							log.Infof("fetchLockDepositEvents - ccid %s (tx_hash: %s) height: %v already on poly",
								hex.EncodeToString(param.CrossChainID), ethTxHash, height)
							continue
						}

						index := big.NewInt(0)
						index.SetBytes(evt.TxId)
						crossTx := &CrossTransfer{
							txIndex: encodeBigInt(index),
							txId:    ethcommon.Hex2Bytes(ethTxHash),
							toChain: uint32(evt.ToChainId),
							value:   []byte(evt.Rawdata),
							height:  height,
						}

						var txHash string
						txHash, err = v.commitVote(uint32(height), crossTx.value, crossTx.txId)
						if err != nil {
							log.Errorf("commitVote failed:%v, height: %d, ethTxHash: %v", err, height, txHash)
							return err
						}
						err = v.waitTx(txHash)
						if err != nil {
							log.Errorf("waitTx failed:%v,height: %d, ethTxHash: %v", err, height, ethTxHash)
							return err
						}
						log.Infof("success side height %d ethTxhash: %v", height, ethTxHash)

					}
				}
			}
		}
	}
	return nil
}
