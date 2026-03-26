#!/usr/bin/env bash

if [ $# != 6 ]; then
	echo "how to use: ./repeat_single_test.sh test_name repeat test_duration nemesis persistent license"
	exit 1
fi

test_name=$1
repeat=$2
test_duration=$3
nemesis=$4
persistent=$5
license=$6
cp_direct_to_leader_routing=$7

lein run test --workload "${test_name}" --test-count "${repeat}" --time-limit "${test_duration}" \
  --nemesis "${nemesis}" --license "${license}" --persistent "${persistent}" \
  --cp-direct-to-leader-routing "${cp_direct_to_leader_routing}"
