# scenarios that need thought

* atm we rely on the `rollback` hook to roll back all the canaries if one canary fails, but what if all the canaries fail at the same time and they never call the `rollback` hook?
* currently we have `.spec.timeout` to specify a duration to wait for to receive the `confirm-rollout` webhook for all canaries (if not already received) before approving the rollout. but what about promotion? what if mid-progress, flagger gets evicted in one of the clusters? the canary in that cluster will get stuck and we'll keep waiting forever to receive the `confirm-promotion` webhook for that canary, blocking the promotion of all the other canaries.
* lets assume the cd pipeline for a cluster goes down. right after that, a canary run is triggered in other clusters, which eventually gets approved and the rollout starts. meanwhile, the cd pipeline recovers and a canary run is triggered in that cluster. atm we allow the recovered cluster to join in, but should we allow that?
