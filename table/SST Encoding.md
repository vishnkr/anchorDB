SST Encoding

Max sst size - 256MB
Block size - 1MB
-----Start
<key_len1><key1><valuelen1><value1><key_len2><key2><valuelen2><value2>
<key_len3><key3><valuelen3><value4><key_len4><key4><valuelen4><value4>
    ....
<key_lenN><keyN><valuelenN><valueN><key_lenN+1><keyN+1><valuelenN+1><valueN+1>
-- metadata section / index
<block1OFFSET><firstKeyLen><firstKey>
<block2OFFSET><firstKeyLen><firstKey>
...
<blockMOFFSET><firstKeyLen><firstKey>
-- index footer
<metadata section offset>
<checksum>
<bloomFilter>
