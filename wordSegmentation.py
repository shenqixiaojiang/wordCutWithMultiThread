from __future__ import print_function
import jieba.posseg as ps
import json
from multiprocessing import Pool
import numpy as np
import sys
import os

def wordCutOne(inTuple):
  try:
    line, out_txt_path = inTuple[0], inTuple[1]
    fout = open(out_txt_path, 'a')
    words = line.strip().split("\t")
    if not len(words) == 3:
      fout.write(words[0]+"\tnull\tnull\n")
    else:
      asrID, asrContent, othContent = words[0], words[1], words[2]
      def cutWords(content):
         words = ps.cut(content)
         wordCuts = []
         for w in words:
           if not w.flag == 'x':
             wordCuts.append(w.word.strip())
         if len(wordCuts) == 0:
           return "null"
         else:
           return wordCuts
      wordCuts1 = cutWords(asrContent)
      wordCuts2 = cutWords(othContent)
      fout.write(asrID.encode("utf-8")+"\t"+",".join(wordCuts1).encode("utf-8") + "\t"+",".join(wordCuts2).encode("utf-8")+"\n")
  except:
    fout.write(words[0]+"\tnull\tnull\n")
    #print("Error",inTuple[0])

def wordCutAll(raw_txt_path, processNum=8):
  outList=[]
  lines=open(raw_txt_path,'r').readlines()
  mapList=[]
  makeDir = False
  if os.path.exists('tmp-wordcut/') == False:
      os.mkdir('tmp-wordcut/')
      makeDir = True
  for line in lines:
      lenText = int(len(line) // 100) + 1   ##according to the length
      mapList.append((line, 'tmp-wordcut/tmpfile_'+ str(lenText)))
  pool = Pool(processes=processNum)
  outList = pool.map(wordCutOne, mapList)
  return makeDir
def combine(out_txt_path,makeDir=False):
  fdout = open(out_txt_path,'w')
  for ii in os.listdir('tmp-wordcut/'):
    if ii.startswith('tmpfile'):
        fdin = open('tmp-wordcut/' + ii)
        for jj in fdin:
          fdout.write(jj)
        fdin.close()
  fdout.close()
  if makeDir == True:
    os.system("rm -rf tmp-wordcut/")  
if __name__=="__main__":
  
  raw_txt_path=sys.argv[1]
  out_txt_path=sys.argv[2]
  makeDir = wordCutAll(raw_txt_path, processNum=4)
  combine(out_txt_path,makeDir=makeDir)
