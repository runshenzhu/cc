package main

import (
	"math"
	"math/big"
	"net/http"
	"runtime"
	"time"
)

var X = new(big.Int)

// basic config
const (
	team_info = "Omegaga's Black Railgun,6537-0651-1730"
)

func decode(a string, shift byte) string {
	n := len(a)
	z := make([]byte, n)
	k := int(math.Sqrt(float64(n)))
	now := 0
	for sum := 0; sum < k; sum++ {
		for x := 0; x <= sum; x++ {
			j := x*k + (sum - x)
			z[now] = 'A' + ((a[j] - 'A' + 26 - shift) % 26)
			now++
		}
	}
	for sum := k; sum < (k<<1)-1; sum++ {
		for x := sum - k + 1; x < k; x++ {
			j := x*k + (sum - x)
			z[now] = 'A' + ((a[j] - 'A' + 26 - shift) % 26)
			now++
		}
	}
	return string(z)
}

func q1Handler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	message := r.URL.Query().Get("message")
	keyInt := new(big.Int)
	keyInt.SetString(key, 10)
	y := new(big.Int)
	y.Div(keyInt, X)

	// y_mod := int(y.Mod(y, big.NewInt(25)).Int64())
	z := 1 + byte(y.Int64()%25)
	decode_str := decode(message, z)

	// construct response string
	t := time.Now()
	output_str := team_info + "\n" +
		t.Format("2006-01-02 15:04:05") + "\n" +
		decode_str + "\n"
	w.Write([]byte(output_str))
}

func heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("You are good\n"))
}

func main() {
	X = new(big.Int)
	runtime.GOMAXPROCS(runtime.NumCPU())
	X.SetString("8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773", 10)
	http.HandleFunc("/q1", q1Handler)
	http.HandleFunc("/heartbeat", heartbeatHandler)
	http.ListenAndServe(":80", nil)
}
