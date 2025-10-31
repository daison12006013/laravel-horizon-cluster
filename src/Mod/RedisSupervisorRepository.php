<?php

namespace Daison\LaravelHorizonCluster\Mod;

use Carbon\CarbonImmutable;
use Laravel\Horizon\Repositories\RedisSupervisorRepository as Base;
use Laravel\Horizon\Supervisor;

/**
 * @author Daison Carino <daison12006013@gmail.com>
 */
class RedisSupervisorRepository extends Base
{
    use PipelineToBlockingTrait;

    /**
     * {@inheritDoc}
     */
    public function get(array $names): array
    {
        $records = $this->blocking(function ($pipe) use ($names) {
            foreach ($names as $name) {
                $pipe->hmget('supervisor:' . $name, ['name', 'master', 'pid', 'status', 'processes', 'options']);
            }
        });

        return collect($records)->filter()->map(function ($record) {
            $record = array_values($record);

            return ! $record[0] ? null : (object) [
                'name'      => $record[0],
                'master'    => $record[1],
                'pid'       => $record[2],
                'status'    => $record[3],
                'processes' => json_decode($record[4], true),
                'options'   => json_decode($record[5], true),
            ];
        })->filter()->all();
    }

    /**
     * {@inheritDoc}
     */
    public function forget($names): void
    {
        $names = (array) $names;

        if (empty($names)) {
            return;
        }

        // start of overriding the logic

        /*
        $this->connection()->del(...collect($names)->map(function ($name) {
            return 'supervisor:'.$name;
        })->all());
        */

        foreach (collect($names)->map(function ($name) {
            return 'supervisor:' . $name;
        })->all() as $name) {
            $this->connection()->del($name);
        }

        // end

        $this->connection()->zrem('supervisors', ...$names);
    }

    /**
     * {@inheritDoc}
     */
    public function update(Supervisor $supervisor): void
    {
        $processes = $supervisor->processPools->mapWithKeys(function ($pool) use ($supervisor) {
            return [$supervisor->options->connection . ':' . $pool->queue() => count($pool->processes())];
        })->toJson();

        $this->blocking(function ($pipe) use ($supervisor, $processes) {
            $pipe->hmset(
                'supervisor:' . $supervisor->name,
                [
                    'name'      => $supervisor->name,
                    'master'    => implode(':', explode(':', $supervisor->name, -1)),
                    'pid'       => $supervisor->pid(),
                    'status'    => $supervisor->working ? 'running' : 'paused',
                    'processes' => $processes,
                    'options'   => $supervisor->options->toJson(),
                ]
            );

            $pipe->zadd(
                'supervisors',
                CarbonImmutable::now()->getTimestamp(),
                $supervisor->name
            );

            $pipe->expire('supervisor:' . $supervisor->name, 30);
        });
    }
}
