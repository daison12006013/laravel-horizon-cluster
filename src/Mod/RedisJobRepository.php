<?php

namespace Daison\LaravelHorizonCluster\Mod;

use Carbon\CarbonImmutable;
use Illuminate\Support\Collection;
use Laravel\Horizon\JobPayload;
use Laravel\Horizon\LuaScripts;
use Laravel\Horizon\Repositories\RedisJobRepository as Base;

/**
 * @author Daison Carino <daison12006013@gmail.com>
 */
class RedisJobRepository extends Base
{
    use PipelineToBlockingTrait;

    /**
     * {@inheritDoc}
     */
    public function getJobs(array $ids, $indexFrom = 0): Collection
    {
        $jobs = $this->blocking(function ($pipe) use ($ids) {
            foreach ($ids as $id) {
                $pipe->hmget($id, $this->keys);
            }
        });

        return $this->indexJobs(collect($jobs)->filter(function ($job) {
            $job = is_array($job) ? array_values($job) : null;

            return is_array($job) && $job[0] !== null && $job[0] !== false;
        })->values(), $indexFrom);
    }

    /**
     * {@inheritDoc}
     */
    public function pushed($connection, $queue, JobPayload $payload): void
    {
        $this->blocking(function ($pipe) use ($connection, $queue, $payload) {
            $this->storeJobReference($pipe, 'recent_jobs', $payload);
            $this->storeJobReference($pipe, 'pending_jobs', $payload);

            $time = str_replace(',', '.', microtime(true));

            $pipe->hmset($payload->id(), [
                'id'         => $payload->id(),
                'connection' => $connection,
                'queue'      => $queue,
                'name'       => $payload->decoded['displayName'],
                'status'     => 'pending',
                'payload'    => $payload->value,
                'created_at' => $time,
                'updated_at' => $time,
            ]);

            $pipe->expireat(
                $payload->id(),
                CarbonImmutable::now()->addMinutes($this->pendingJobExpires)->getTimestamp()
            );
        });
    }

    /**
     * {@inheritDoc}
     */
    public function remember($connection, $queue, JobPayload $payload): void
    {
        $this->blocking(function ($pipe) use ($connection, $queue, $payload) {
            $this->storeJobReference($pipe, 'monitored_jobs', $payload);

            $pipe->hmset(
                $payload->id(),
                [
                    'id'           => $payload->id(),
                    'connection'   => $connection,
                    'queue'        => $queue,
                    'name'         => $payload->decoded['displayName'],
                    'status'       => 'completed',
                    'payload'      => $payload->value,
                    'completed_at' => str_replace(',', '.', microtime(true)),
                ]
            );

            $pipe->expireat(
                $payload->id(),
                CarbonImmutable::now()->addMinutes($this->monitoredJobExpires)->getTimestamp()
            );
        });
    }

    /**
     * {@inheritDoc}
     */
    public function migrated($connection, $queue, Collection $payloads): void
    {
        $this->blocking(function ($pipe) use ($payloads) {
            foreach ($payloads as $payload) {
                $pipe->hmset(
                    $payload->id(),
                    [
                        'status'     => 'pending',
                        'payload'    => $payload->value,
                        'updated_at' => str_replace(',', '.', microtime(true)),
                    ]
                );
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    public function completed(JobPayload $payload, $failed = false, $silenced = false): void
    {
        if ($payload->isRetry()) {
            $this->updateRetryInformationOnParent($payload, $failed);
        }

        $this->blocking(function ($pipe) use ($payload, $silenced) {
            $this->storeJobReference($pipe, $silenced ? 'silenced_jobs' : 'completed_jobs', $payload);
            $this->removeJobReference($pipe, 'pending_jobs', $payload);

            $pipe->hmset(
                $payload->id(),
                [
                    'status'       => 'completed',
                    'completed_at' => str_replace(',', '.', microtime(true)),
                ]
            );

            $pipe->expireat($payload->id(), CarbonImmutable::now()->addMinutes($this->completedJobExpires)->getTimestamp());
        });
    }

    /**
     * {@inheritDoc}
     */
    public function deleteMonitored(array $ids): void
    {
        $this->blocking(function ($pipe) use ($ids) {
            foreach ($ids as $id) {
                $pipe->expireat($id, CarbonImmutable::now()->addDays(7)->getTimestamp());
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    public function trimRecentJobs(): void
    {
        $this->blocking(function ($pipe) {
            $pipe->zremrangebyscore(
                'recent_jobs',
                CarbonImmutable::now()->subMinutes($this->recentJobExpires)->getTimestamp() * -1,
                '+inf'
            );

            $pipe->zremrangebyscore(
                'recent_failed_jobs',
                CarbonImmutable::now()->subMinutes($this->recentFailedJobExpires)->getTimestamp() * -1,
                '+inf'
            );

            $pipe->zremrangebyscore(
                'pending_jobs',
                CarbonImmutable::now()->subMinutes($this->pendingJobExpires)->getTimestamp() * -1,
                '+inf'
            );

            $pipe->zremrangebyscore(
                'completed_jobs',
                CarbonImmutable::now()->subMinutes($this->completedJobExpires)->getTimestamp() * -1,
                '+inf'
            );

            $pipe->zremrangebyscore(
                'silenced_jobs',
                CarbonImmutable::now()->subMinutes($this->completedJobExpires)->getTimestamp() * -1,
                '+inf'
            );
        });
    }

    /**
     * {@inheritDoc}
     */
    public function failed($exception, $connection, $queue, JobPayload $payload): void
    {
        $this->blocking(function ($pipe) use ($exception, $connection, $queue, $payload) {
            $this->storeJobReference($pipe, 'failed_jobs', $payload);
            $this->storeJobReference($pipe, 'recent_failed_jobs', $payload);
            $this->removeJobReference($pipe, 'pending_jobs', $payload);
            $this->removeJobReference($pipe, 'completed_jobs', $payload);
            $this->removeJobReference($pipe, 'silenced_jobs', $payload);

            $pipe->hmset(
                $payload->id(),
                [
                    'id'         => $payload->id(),
                    'connection' => $connection,
                    'queue'      => $queue,
                    'name'       => $payload->decoded['displayName'],
                    'status'     => 'failed',
                    'payload'    => $payload->value,
                    'exception'  => (string) $exception,
                    'context'    => method_exists($exception, 'context')
                        ? json_encode($exception->context())
                        : null,
                    'failed_at' => str_replace(',', '.', microtime(true)),
                ]
            );

            $pipe->expireat(
                $payload->id(),
                CarbonImmutable::now()->addMinutes($this->failedJobExpires)->getTimestamp()
            );
        });
    }

    /**
     * {@inheritDoc}
     */
    public function purge($queue)
    {
        if (config('horizon.eval.purge', true)) {
            return $this->connection()->eval(
                LuaScripts::purge(),
                2,
                'recent_jobs',
                'pending_jobs',
                config('horizon.prefix'),
                $queue
            );
        }

        return $this->purgeViaPhp($queue);
    }

    /**
     * This should be the equivalent of LuaScripts::purge()
     *
     * @see vendor/laravel/horizon/src/LuaScripts.php
     */
    public function purgeViaPhp($queue)
    {
        $key1 = 'recent_jobs';
        $key2 = 'pending_jobs';
        $arg1 = config('horizon.prefix');
        $arg2 = $queue;

        return $this->blocking(function ($pipe) use ($key1, $key2, $arg1, $arg2) {
            $count  = 0;
            $cursor = 0;

            do {
                // Iterate over the recent jobs sorted set
                $scanner = $pipe->zscan($key1, $cursor);
                $cursor  = $scanner[0];

                foreach ($scanner[1] as $i => $jobid) {
                    $hashkey = $arg1 . $jobid;
                    $job     = $pipe->hmget($hashkey, ['status', 'queue']);

                    // Delete the pending/reserved jobs that match the queue
                    // name from the sorted sets as well as the job hash
                    if (($job[0] === 'reserved' || $job[0] === 'pending') && $job[1] === $arg2) {
                        $pipe->zrem($key1, $jobid);
                        $pipe->zrem($key2, $jobid);
                        $pipe->del($hashkey);
                        $count++;
                    }
                }
            } while ($cursor !== '0' && $cursor !== 0);

            return $count;
        });
    }
}
