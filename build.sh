#!/usr/bin/env bash

set -o errexit

# older version Git don't support --short !
if [ -d ".git" ];then
    #branch=`git symbolic-ref --short -q HEAD`
    branch=$(git symbolic-ref -q HEAD | awk -F'/' '{print $3;}')
    cid=$(git rev-parse HEAD)
else
    branch="unknown"
    cid="0.0"
fi
branch=$branch","$cid

# make sure we're in the directory where the script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GOPATH=$(pwd)
export GOPATH

info="main.VERSION=$branch"
# golang version
goversion=$(go version | awk -F' ' '{print $3;}')
info=$info","$goversion
bigVersion=$(echo $goversion | awk -F'[o.]' '{print $2}')
midVersion=$(echo $goversion | awk -F'[o.]' '{print $3}')
if  [ $bigVersion -lt "1" -o $bigVersion -eq "1" -a $midVersion -lt "9" ]; then
    echo "go version[$goversion] must >= 1.9"
    exit 1
fi

t=$(date "+%Y-%m-%d_%H:%M:%S")
info=$info","$t

output=./bin/
rm -rf ${output}

echo "[ BUILD RELEASE ]"
run_builder='go build -v'
#goos=(windows darwin linux)
#for g in "${goos[@]}"; do
#    export GOOS=$g
#    echo "try build goos=$g"
#    $run_builder -ldflags "-X $info" -o "$output/redis-full-check.$g" "./src/full_check/"
#    echo "build successfully!"
#done

#echo "all build successfully!"


$run_builder -ldflags "-X $info" -o "$output/redis-full-check" "./src/full_check/"
echo "build successfully!"
