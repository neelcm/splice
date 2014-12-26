# auto.sh 0.1
# Neel Mouleeswaran 06/05/2014
# automates the process of getting the latest blockchain data and loading it into solr

echo "--> cleaning environment..."

#cleanup
`rm blocks.csv`
`rm block_seg_*`

echo "--> grabbing latest blockchain info..."

# grab height from bitcoind log, write to dat savepoint
# height=`tail -n5000 ~/Library/Application\ Support/Bitcoin/debug.log | grep height= | awk '{print $6}' | cut -d '=' -f 2 | tail -n1`

echo "--> parsing blockchain..."

#launch blockchain parser
#`go run blockchain_parser.go $height`
`go run blockchain_parser.go`

echo "--> loading into solr..."

#load into solr
`split -l 500001 blocks.csv block_seg_`
#`java -Dtype=text/csv -Durl="http://localhost:8983/solr/collection3/update" -jar post.jar block_seg_*`

echo "--> cleaning up..."
#cleanup
#`rm blocks.csv`
#`rm block_seg_*`

echo "--> done!"

exit
