#!/bin/bash

# Reference: https://zhuanlan.zhihu.com/p/351897096

db_set() {
  echo "$1,$2" >>database
}

db_get() {
  grep "^$1," database | sed -e "s/^$1,//" | tail -n 1
}
