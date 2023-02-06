#!/usr/bin/env bash

set -o errexit

fullcheckVersion=$(cat ChangeLog | head -n 2 | grep VERSION | awk '{print $3}')

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

output=$(pwd)/bin/
rm -rf ${output}

echo "[ BUILD RELEASE ]"
run_builder='go build -v'

#cd src/full_check
#goos=(windows darwin linux)
#for g in "${goos[@]}"; do
#    export GOOS=$g
#    echo "try build goos=$g"
#    $run_builder -ldflags "-X $info" -o "$output/redis-full-check.$g"
#    unset GOOS
#    echo "build successfully!"
#done
#cp $output/../ChangeLog $output
#cd $output
#tar -cvzf redis-full-check-"$fullcheckVersion".tar.gz redis-full-check.darwin redis-full-check.linux redis-full-check.windows ChangeLog

cd src/full_check
$run_builder -ldflags "-X $info" -o "$output/redis-full-check"
echo "build successfully!"
