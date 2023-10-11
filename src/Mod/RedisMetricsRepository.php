<?php

namespace Daison\LaravelHorizonCluster\Mod;

use Laravel\Horizon\LuaScripts;
use Laravel\Horizon\Repositories\RedisMetricsRepository as Base;

class RedisMetricsRepository extends Base
{
    use PipelineToBlockingTrait;

    /**
     * @inheritDoc
     */
    protected function baseSnapshotData($key)
    {
        /*
        $responses = $this->connection()->transaction(function ($trans) use ($key) {
            $trans->hmget($key, ['throughput', 'runtime']);

            $trans->del($key);
        });
        */

        $responses = $this->blocking(function ($pipe) use ($key) {
            $pipe->hmget($key, ['throughput', 'runtime']);
            $pipe->del($key);
        });

        $snapshot = array_values($responses[0]);

        return [
            'throughput' => $snapshot[0],
            'runtime'    => $snapshot[1],
        ];
    }

    /**
     * This should be the equivalent of LuaScripts::updateMetrics()
     *
     * @see vendor/laravel/horizon/src/LuaScripts.php
     */
    protected function updateMetrics($key1, $key2, $arg1): void
    {
        $this->blocking(function ($pipe) use ($key1, $key2, $arg1) {
            // Initialize throughput to 0 if it does not exist
            $pipe->hsetnx($key1, 'throughput', 0);

            // Add KEYS[1] to the set represented by KEYS[2]
            $pipe->sadd($key2, $key1);

            // Get the 'throughput' and 'runtime' values from the hash
            $hash = $pipe->hmget($key1, 'throughput', 'runtime');

            $throughput = $hash[0] + 1;
            $runtime    = 0;

            if ($hash[1]) {
                $runtime = (($hash[0] * (float) $hash[1]) + (float) $arg1) / $throughput;
            } else {
                $runtime = (float) $arg1;
            }

            // Update the 'throughput' and 'runtime' values in the hash
            $pipe->hmset($key1, 'throughput', $throughput, 'runtime', $runtime);
        });
    }

    /**
     * @inheritDoc
     */
    public function incrementJob($job, $runtime)
    {
        if (config('horizon.eval.increment_job', true)) {
            $this->connection()->eval(
                LuaScripts::updateMetrics(),
                2,
                'job:' . $job,
                'measured_jobs',
                str_replace(',', '.', (string) $runtime)
            );

            return;
        }

        $this->updateMetrics(
            'job:' . $job,
            'measured_jobs',
            str_replace(',', '.', (string) $runtime),
        );
    }

    /**
     * @inheritDoc
     */
    public function incrementQueue($queue, $runtime)
    {
        if (config('horizon.eval.increment_queue', true)) {
            $this->connection()->eval(
                LuaScripts::updateMetrics(),
                2,
                'queue:' . $queue,
                'measured_queues',
                str_replace(',', '.', (string) $runtime)
            );

            return;
        }

        $this->updateMetrics(
            'queue:' . $queue,
            'measured_queues',
            str_replace(',', '.', (string) $runtime),
        );
    }
}
