#!/usr/bin/env bash

ssh-keyscan -t ssh-ed25519 n1 >> ~/.ssh/known_hosts
ssh-keyscan -t ssh-ed25519 n2 >> ~/.ssh/known_hosts
ssh-keyscan -t ssh-ed25519 n3 >> ~/.ssh/known_hosts
ssh-keyscan -t ssh-ed25519 n4 >> ~/.ssh/known_hosts
ssh-keyscan -t ssh-ed25519 n5 >> ~/.ssh/known_hosts

tests=("non-reentrant-lock" "reentrant-lock" "non-reentrant-fenced-lock" "reentrant-fenced-lock" "semaphore" "id-gen-long" "cas-long" "cas-reference" "cas-cp-map" "snapshot-stress" "cp-map-purge-lite-member")

if [ $# -lt 3 ]; then
	echo "Usage: ./repeat_all_cp_tests.sh repeat test_duration license [tests...]"
	echo "Tests: ${tests[*]}"
	exit 1
fi

repeat=$1
test_duration=$2
license=$3
cp_direct_to_leader_routing=$4


if [ $# -gt 4 ]; then
  # Just run specified tests...
  tests=()
  for i in "${@:5}"
  do
    tests+=("$i")
  done
fi

run_single_test () {
    test_name=$1
    nemesis=$2
    persistent=$3
    cp_direct_to_leader_routing=$4
    echo "Running '$test_name' test with '$nemesis' nemesis, persistent=$persistent, cp_direct_to_leader_routing=$cp_direct_to_leader_routing"

    lein run test --workload "${test_name}" --time-limit "${test_duration}" --license "${license}" --nemesis "${nemesis}" --persistent "${persistent}" --cp-direct-to-leader-routing "${cp_direct_to_leader_routing}"

    if [ $? != '0' ]; then
        echo "'$test_name' test failed"
        exit 1
    fi
}

round=1
echo "Will run [${tests[*]}] tests..."

while [ ${round} -le ${repeat} ]; do

    echo "round: $round"

    for test in "${tests[@]}"
    do
      run_single_test "${test}" "partition" "false" "false"
      run_single_test "${test}" "partition" "false" "true"
      run_single_test "${test}" "partition" "true" "false"
      run_single_test "${test}" "restart-majority" "true" "false"
      run_single_test "${test}" "restart-majority" "true" "true"
    done

    ((round++))

done
