// 测试
// author: baoqiang
// time: 2019-04-24 18:34
package xdiskq

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestGo(t *testing.T) {
	GoRandom()
}

// uint8  : 0 to 255
// uint16 : 0 to 65535
// uint32 : 0 to 4294967295
// uint64 : 0 to 18446744073709551615
// int8   : -128 to 127
// int16  : -32768 to 32767
// int32  : -2147483648 to 2147483647
// int64  : -9223372036854775808 to 9223372036854775807
//18,446,744,073,709,551,615
//1844,6744,0737,0955,1615
//万兆,兆,亿,万,个
func GoRandom() {
	for range [10]int{} {
		r := rand.Int()
		fmt.Println(r)
	}
}

