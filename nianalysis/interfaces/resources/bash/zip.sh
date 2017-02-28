#!/usr/bin/env bash
# Zips a directory relative to its parent (i.e. missing out intermediate directories)

cwd=`pwd`
pushd `dirname $2` 
zip -rq $cwd/$1 `basename $2`
popd
