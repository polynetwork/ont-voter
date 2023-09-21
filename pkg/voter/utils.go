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
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ontology_go_sdk "github.com/ontio/ontology-go-sdk"
	ontcommon "github.com/ontio/ontology-go-sdk/common"
	polycommon "github.com/polynetwork/poly/common"
	"math/big"
)

func ontevmGetCurrentHeight(client *ontology_go_sdk.OntologySdk) (height uint64, err error) {
	h, err := client.GetCurrentBlockHeight()
	return uint64(h), err
}

func HexStringReverse(value string) string {
	aa, _ := hex.DecodeString(value)
	bb := HexReverse(aa)
	return hex.EncodeToString(bb)
}

func HexReverse(arr []byte) []byte {
	l := len(arr)
	x := make([]byte, 0)
	for i := l - 1; i >= 0; i-- {
		x = append(x, arr[i])
	}
	return x
}

type StorageLog struct {
	Address common.Address
	Topics  []common.Hash
	Data    []byte
}

func (self *StorageLog) Deserialization(source *polycommon.ZeroCopySource) error {
	address, eof := source.NextAddress()
	if eof {
		return fmt.Errorf("StorageLog.address eof")
	}
	self.Address = common.Address(address)
	l, eof := source.NextUint32()
	if eof {
		return fmt.Errorf("StorageLog.l eof")
	}
	self.Topics = make([]common.Hash, 0, l)
	for i := uint32(0); i < l; i++ {
		h, _ := source.NextHash()
		if eof {
			return fmt.Errorf("StorageLog.h eof")
		}
		self.Topics = append(self.Topics, common.Hash(h))
	}
	data, eof := source.NextVarBytes()
	if eof {
		return fmt.Errorf("StorageLog.Data eof")
	}
	self.Data = data
	return nil
}

func deserializeStorageLog(notify *ontcommon.NotifyEventInfo) (storageLog StorageLog, err error) {
	states, ok := notify.States.(string)
	if !ok {
		err = fmt.Errorf("err States.(string)")
		return
	}
	var data []byte
	data, err = hexutil.Decode(states)
	if err != nil {
		return
	}
	source := polycommon.NewZeroCopySource(data)
	err = storageLog.Deserialization(source)
	if err != nil {
		return
	}
	if len(storageLog.Topics) == 0 {
		err = fmt.Errorf("err storageLog.Topics is 0")
		return
	}
	return
}

func encodeBigInt(b *big.Int) string {
	if b.Uint64() == 0 {
		return "00"
	}
	return hex.EncodeToString(b.Bytes())
}
