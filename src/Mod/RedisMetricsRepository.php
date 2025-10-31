<?php

namespace Daison\LaravelHorizonCluster\Mod;

use Illuminate\Support\Str;
use Laravel\Horizon\LuaScripts;
use Laravel\Horizon\Repositories\RedisMetricsRepository as Base;

class RedisMetricsRepository extends Base
{
    use PipelineToBlockingTrait;
    use HashTagTrait;

    /**
     * {@inheritDoc}
     */
    protected function baseSnapshotData($key): array
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
     * {@inheritDoc}
     */
    public function incrementJob($job, $runtime): void
    {
        if (config('horizon.eval.increment_job', true)) {
            $this->connection()->eval(
                LuaScripts::updateMetrics(),
                2,
                "job:{$this->getHashTag()}:{$job}",
                "measured_jobs:{$this->getHashTag()}",
                str_replace(',', '.', (string) $runtime)
            );

            return;
        }

        $this->updateMetrics(
            "job:{$this->getHashTag()}:{$job}",
            "measured_jobs:{$this->getHashTag()}",
            str_replace(',', '.', (string) $runtime),
        );
    }

    /**
     * {@inheritDoc}
     */
    public function incrementQueue($queue, $runtime): void
    {

        if (config('horizon.eval.increment_queue', true)) {
            $this->connection()->eval(
                LuaScripts::updateMetrics(),
                2,
                "queue:{$this->getHashTag()}:{$queue}",
                "measured_queues:{$this->getHashTag()}",
                str_replace(',', '.', (string) $runtime)
            );

            return;
        }

        $this->updateMetrics(
            "queue:{$this->getHashTag()}:{$queue}",
            "measured_queues:{$this->getHashTag()}",
            str_replace(',', '.', (string) $runtime),
        );
    }

    /**
     * {@inheritDoc}
     */
    public function measuredQueues(): array
    {
        $queues = (array) $this->connection()->smembers("measured_queues:{$this->getHashTag()}");

        $pattern = '/queue:'.$this->getHashTag().':(.*)/';

        return collect($queues)
            ->map(fn ($class) => preg_match($pattern, $class, $matches) ? $matches[1] : $class)
            ->sort()
            ->values()
            ->all();
    }

    /**
     * {@inheritDoc}
     */
    public function clear(): void
    {
        $this->forget('last_snapshot_at');
        $this->forget("measured_jobs:{$this->getHashTag()}");
        $this->forget("measured_queues:{$this->getHashTag()}");
        $this->forget('metrics:snapshot');

        foreach (['queue:*', 'job:*', 'snapshot:*'] as $pattern) {
            $cursor = null;

            do {
                [$cursor, $keys] = $this->connection()->scan(
                    $cursor ?? 0, ['match' => config('horizon.prefix').$pattern]
                );

                foreach ($keys ?? [] as $key) {
                    $this->forget(Str::after($key, config('horizon.prefix')));
                }
            } while ($cursor > 0);
        }
    }
}
