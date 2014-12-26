/**
    blockchain_parser: written in Go - utilizes gocoin library
    
    @author Neel Mouleeswaran
    @version 0.3 06/06/2014

    current functionality: 
        1) reads blockchain(dat) store directly from file and extracts block + tx information
        2) exports tx info to csv called blocks.csv
        3) has a savepoint and seeks to the most recent block before parsing
        4) handles the once-in-a-blue-moon orphan blocks


    notes:
        .seek.dat contains the hash of the last parent seen. this means that for example, the last block
        parsed was 303348, it would write the hash of it's parent (0000000000000000345dd7c1c9a7a7897026b6419daf9ea00f74d827b8bed48a)
        to the savepoint, and upon relaunching the parser, it will pick up from block 303349 
*/

package main
import (
    "github.com/piotrnar/gocoin/lib/btc"
    "github.com/piotrnar/gocoin/lib/others/blockdb"
    
    "fmt"
    "encoding/hex"
    "strconv"
    
    "os"
    "io/ioutil"
    "bufio"
)

func main() {

    /* setup seek */
    dat_file, err := os.Open(".seek.dat")
    if err != nil { panic(err) }
    
    var lines []string
    scanner := bufio.NewScanner(dat_file)
    
    for scanner.Scan() {
        lines = append(lines, scanner.Text())
    }

    last_parent := lines[0]

    fmt.Printf("last parent = %v", last_parent)

    /* setup write */
    header := "id,block_number,block_time,merkle_root,tx_size,num_inputs,tx_inputs,num_outputs,tx_outputs,amount_transacted\n"
    
    // open output file
    of, err := os.Create("blocks.csv")
    if err != nil { panic(err) }

    // close outfile on exit
    defer func() {
        if err := of.Close(); err != nil {
            panic(err)
        }
    }()

    // magic number for mainnet
    magicNumber := [4]byte{0xF9,0xBE,0xB4,0xD9}

    // point to block directory (obtained from bitcoind)
    BlockDatabase := blockdb.NewBlockDB("/Users/neelcm/Library/Application Support/Bitcoin/blocks", magicNumber)

    of.Write([]byte(header))

    var num_lines_written uint64
    num_lines_written = 0

    fmt.Printf("last parent hash parsed: %v\nseeking blockchain...\n", last_parent)

    var i int64

    var block_parent string
    var block_number int64
    
    var dat []byte
    var dat_err error

    var bl *btc.Block
    var block_err error

    var reached_last_parent bool


    reached_last_parent = false

    // seek blockdb
    var seek_count int64
    seek_count = -1

    /* seek loop */
    for {

        dat, dat_err = BlockDatabase.FetchNextBlock() // fetch block

        if dat == nil || dat_err != nil {
            fmt.Println("end of db reached\n")
            break
        }

        bl, block_err = btc.NewBlock(dat[:]) // decode block
        
        if block_err != nil {
            fmt.Println("block error!\n")
        }

        parent_hash := btc.NewUint256(bl.ParentHash()).String() // get block hash

       // fmt.Printf("%v : %v | %v\n", seek_count, parent_hash, last_parent)

        // check if block hash matches hash we are looking for
        if parent_hash == last_parent {
            
            // toggle bool, as now we need to keep fetching
            // until we no longer see the same parent hash
            if reached_last_parent == false {
                reached_last_parent = true
                fmt.Printf("found last parent - %v\n", parent_hash)
            } else {
                
                // filter orphan blocks until next block on main chain found
                for parent_hash == last_parent {

                    // fetch and decode the next block
                    
                    dat, dat_err = BlockDatabase.FetchNextBlock()

                    bl, block_err = btc.NewBlock(dat[:])

                    parent_hash = btc.NewUint256(bl.ParentHash()).String()

                }

                fmt.Printf("found stop point - %v\n", parent_hash)
                break
            }
        } else {

            if reached_last_parent == true {
                fmt.Printf("found stop point - %v\n", parent_hash)
                seek_count-- // loop was run one extra time for the double check
                break
            }
        }

        fmt.Printf("seek count = %v\n", seek_count)

        seek_count++

    }

    start_block := seek_count - 1

    /* parse loop */
    for i = start_block; i < 1000000; i++ {

        if i % 1000 == 0 { 
            fmt.Printf("%d\n", i);
        }

        // entering the parse loop, the current dat is already the block we need to analyze
        if i == start_block {

            block_parent = btc.NewUint256(bl.ParentHash()).String()
            block_number = start_block

            fmt.Printf("i = %v block number = %v, parent_hash = %v\n", i, block_number, block_parent)

        } else {

            dat, dat_err = BlockDatabase.FetchNextBlock()

            if dat == nil || dat_err != nil {
                fmt.Println("end of db reached\n")
                break
            }

            bl, block_err = btc.NewBlock(dat[:])

            // increment block number and update parent hash if not an orphan block
            if block_parent != btc.NewUint256(bl.ParentHash()).String() {
                block_parent = btc.NewUint256(bl.ParentHash()).String()
                block_number++
            }

        }

        bl, block_err = btc.NewBlock(dat[:])

        if block_err != nil {
            println("block is inconsistent:", block_err.Error())
            break
        }

        // fetch TXs and iterate over them
        bl.BuildTxList()

        // fmt.Printf("%v, %v\n", block_number, len(bl.Txs))
        
        for _, tx := range bl.Txs {

            /*
            * IsCoinBase determines whether or not a transaction is a coinbase. 
            * A coinbase is a special transaction created by miners that has no inputs. 
            * This is represented in the block chain by a transaction with a single input that has a previous 
            * output transaction index set to the maximum value along with a zero hash.
            *
            * --> This code intentionally avoids parsing coinbase(s)
            */

            if tx.IsCoinBase() {
                // newly generated coin
            } else {

            /* tx info */

                // tx hash, block #, block time, merkle root, tx size
                tx_info := [5]string{tx.Hash.String(),
                                    strconv.Itoa(int(block_number)),
                                    strconv.FormatInt(int64(bl.BlockTime()), 10),
                                    hex.EncodeToString(bl.MerkleRoot()),  
                                    strconv.Itoa(int(tx.Size))}                                

                // write tx_info to file
                for j:= 0; j < len(tx_info); j++ {
                    of.Write([]byte("\""))
                    of.Write([]byte(tx_info[j]))
                    of.Write([]byte("\""))
                    of.Write([]byte(","))
                }


            /* tx inputs */

                // write num_inputs
                of.Write([]byte("\""))
                of.Write([]byte(strconv.Itoa(len(tx.TxIn))))
                of.Write([]byte("\""))
                of.Write([]byte(","))

                of.Write([]byte("\""))

                for txin_index, txin := range tx.TxIn {

                    // multiple input delimiter
                    if txin_index > 0 {
                        of.Write([]byte(";"))
                    }

                    // input hash
                    tx_inputs := [1]string{txin.Input.String()}

                    // write tx_inputs to file
                    for k:=0; k < 1; k++ {
                        of.Write([]byte(tx_inputs[k]))
                    }

                }

                of.Write([]byte("\""))
                of.Write([]byte(","))

            /* tx outputs */

                // write num_outputs
                of.Write([]byte("\""))
                of.Write([]byte(strconv.Itoa(len(tx.TxOut))))
                of.Write([]byte("\""))
                of.Write([]byte(","))

                of.Write([]byte("\""))

                var btc_transacted uint64

                btc_transacted = 0  

                for txo_index, txout := range tx.TxOut {

                    if txo_index > 0 {
                        of.Write([]byte(";"))
                    }

                    hex.EncodeToString(txout.Pk_script)

                    var txout_addr_string string
                    txout_addr := btc.NewAddrFromPkScript(txout.Pk_script, false)

                    if txout_addr == nil {
                        txout_addr_string = "txout_addr_decode_error"
                    } else {
                        txout_addr_string = txout_addr.String()
                    }

                    tx_outputs := [2]string{txout_addr_string,
                                            strconv.Itoa(int(txout.Value))}

                    btc_transacted += txout.Value

                    // write tx_outputs to file
                    for l:=0; l < 2; l++ {
                        of.Write([]byte(tx_outputs[l]))
                        
                        if l != 1 {
                            of.Write([]byte(","))
                        }
                    }

                }

                of.Write([]byte("\""))
                
                // write btc_transacted
                of.Write([]byte(","))
                of.Write([]byte("\""))
                of.Write([]byte(strconv.FormatUint(btc_transacted, 10)))
                of.Write([]byte("\""))


            } 

            if tx.IsCoinBase() == false {
                num_lines_written++
                if(num_lines_written % 500000 == 0) {
                    of.Write([]byte("\n"))
                    of.Write([]byte(header))
                    fmt.Printf("%s", "written 500k lines")
                } else {
                    of.Write([]byte("\n"))
                }
            }

        }
        

    }

    /* update .seek.dat */
    fmt.Printf("last block: %v | hash: %v\n", strconv.FormatInt(block_number, 10), block_parent)
    ioutil.WriteFile(".seek.dat", []byte(block_parent), 0644) // 0644 = overwrite
    dat_file.Close()
}