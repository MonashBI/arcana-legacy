#!/usr/bin/env bash
# TAR_GZ a directory relative to its parent (i.e. missing out intermediate directories)

cwd=`pwd`
pushd `dirname $2`
tar -zcvf $cwd/$1.tar.gz `basename $2`
popd

