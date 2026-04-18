#!/bin/bash
go run main/wc.go master sequential main/pg-*.txt 
sort -n -k2 mrtmp.wcseq | tail -10 | diff - main/mr-testout.txt > diff.out 
if [ -s diff.out ]
then
echo "Failed test. Output should be as in main/mr-testout.txt. Your output differs as follows (from diff.out):" > /dev/stderr
  cat diff.out
else
  echo "Passed test" > /dev/stderr
fi
rm mrtmp.wcseq* 
rm diff.out
