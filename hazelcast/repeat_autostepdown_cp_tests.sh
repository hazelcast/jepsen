#!/usr/bin/env bash

ssh-keyscan -t ssh-ed25519 n1 >> ~/.ssh/known_hosts 2>/dev/null
ssh-keyscan -t ssh-ed25519 n2 >> ~/.ssh/known_hosts 2>/dev/null
ssh-keyscan -t ssh-ed25519 n3 >> ~/.ssh/known_hosts 2>/dev/null
ssh-keyscan -t ssh-ed25519 n4 >> ~/.ssh/known_hosts 2>/dev/null
ssh-keyscan -t ssh-ed25519 n5 >> ~/.ssh/known_hosts 2>/dev/null

# Default set of CP workloads we care about for auto-step-down
tests=("non-reentrant-fenced-lock" \
       "reentrant-fenced-lock" \
       "semaphore" \
       "cas-cp-map" \
       "id-gen-long")

if [ $# -lt 3 ]; then
  echo "Usage: ./repeat_autostepdown_cp_tests.sh repeat test_duration_seconds license [tests...]"
  echo "Default tests: ${tests[*]}"
  exit 1
fi

repeat=$1
test_duration=$2
license=$3

# If extra args are given, treat them as the list of tests to run
if [ $# -gt 3 ]; then
  tests=()
  for i in "${@:4}"; do
    tests+=("$i")
  done
fi

run_single_test () {
    test_name=$1
    nemesis=$2
    persistent=$3
    cp_direct_to_leader_routing=$4
    step_down=$5

    echo "Running '$test_name' test with nemesis='$nemesis', persistent=$persistent, cp_direct_to_leader_routing=$cp_direct_to_leader_routing, step_down=$step_down"

    lein run test \
      --workload "${test_name}" \
      --time-limit "${test_duration}" \
      --license "${license}" \
      --nemesis "${nemesis}" \
      --persistent "${persistent}" \
      --cp-direct-to-leader-routing "${cp_direct_to_leader_routing}" \
      --step-down-when-leader "${step_down}"

    if [ $? -ne 0 ]; then
        echo "'$test_name' test FAILED for nemesis='${nemesis}', persistent=${persistent}, cp_direct_to_leader_routing=${cp_direct_to_leader_routing}, step_down=${step_down}"
        exit 1
    fi
}

round=1
echo "Will run tests: [${tests[*]}]"
echo "Each scenario: time-limit=${test_duration}s, step-down-when-leader=n1 (always)"

while [ "${round}" -le "${repeat}" ]; do
    echo "=== round: ${round} ==="

    for test in "${tests[@]}"; do
      # Always have an auto-step-down leader (n1)
      step_down="n1"

      # Keep the matrix modest so total fits in ~6h for long durations.
      # 1) partition, non-persistent
      run_single_test "${test}" "partition" "false" "false" "${step_down}"

      # 2) partition, persistent
      run_single_test "${test}" "partition" "true"  "false" "${step_down}"

      # 3) restart-majority, persistent
      run_single_test "${test}" "restart-majority" "true"  "false" "${step_down}"

      # 4) restart-majority, non-persistent
      run_single_test "${test}" "restart-majority" "false" "false" "${step_down}"
    done

    ((round++))
done