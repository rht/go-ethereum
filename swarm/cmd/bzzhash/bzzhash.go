// bzzhash
package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"sync"

	"github.com/ethereum/go-ethereum/swarm/storage"
	"github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/ipfs/go-datastore/flatfs"
	"github.com/ipfs/go-ipfs/blocks"
	bstore "github.com/ipfs/go-ipfs/blocks/blockstore"
)

var defaultHash = "SHA256"
var defaultDbCapacity = uint64(50000000)
var defaultRadius = 0

const kSizeBlockstoreWriteCache = 100

//func initDbStore() (m *storage.LocalStore) {
func initDbStore() bstore.Blockstore {
	//os.RemoveAll("/tmp/bzz")
	ds, err := flatfs.New("/tmp/bzz", 4, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}

	m, err := bstore.WriteCached(bstore.NewBlockstore(ds), kSizeBlockstoreWriteCache)
	//hash := storage.MakeHashFunc(defaultHash)
	//m, err := storage.NewDbStore("/tmp/bzz", hash, defaultDbCapacity, defaultRadius)
	//m, err := storage.NewLocalStore(hash, storage.NewStoreParams("/tmp/bzz"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		panic("no dbStore")
	}
	return m
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	if len(os.Args) < 2 {
		fmt.Println("Usage: bzzhash <file name>")
		os.Exit(0)
	}
	name := os.Args[1]
	stat, err := os.Stat(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	if stat.IsDir() {
		files, err := ioutil.ReadDir(stat.Name())
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		cwd, _ := os.Getwd()
		for _, fi := range files {
			putFile(cwd+"/"+name+"/"+fi.Name(), fi.Size())
		}
		return
	}
	putFile(name, stat.Size())
}

func putFile(name string, size int64) {
	f, err := os.Open(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		fmt.Println("Error opening file " + name)
		os.Exit(1)
	}
	sr := io.NewSectionReader(f, 0, size)
	chunker := storage.NewTreeChunker(storage.NewChunkerParams())
	hash := make([]byte, chunker.KeySize())
	m := initDbStore()
	//defer m.Close()

	wg := &sync.WaitGroup{}
	chunkC := make(chan *storage.Chunk)
	errC := chunker.Split(hash, sr, chunkC, wg)
	wg.Wait()
SPLIT:
	for {
		select {
		case chunk := <-chunkC:
			//fmt.Fprintf(os.Stderr, "aaa")
			m.Put(blocks.NewBlock(chunk.SData))
			//m.Put(blocks.NewBlockWithHash(chunk.SData, chunk.Key.Hex))
			//m.Put(chunk)
		case err, ok := <-errC:
			if err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
			}
			if !ok {
				fmt.Printf("%064x\n", hash)
				break SPLIT
			}
		}
	}

}
