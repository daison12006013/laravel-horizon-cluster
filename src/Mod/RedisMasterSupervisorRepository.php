<?php

namespace Daison\LaravelHorizonCluster\Mod;

use Carbon\CarbonImmutable;
use Laravel\Horizon\MasterSupervisor;
use Laravel\Horizon\Repositories\RedisMasterSupervisorRepository as Base;

/**
 * @author Daison Carino <daison12006013@gmail.com>
 */
class RedisMasterSupervisorRepository extends Base
{
    use PipelineToBlockingTrait;

    /**
     * {@inheritDoc}
     *
     * This method overrides the original logic of horizon, pipeline is not working
     * properly with redis cluster. Therefore, we're going to blocking i/o approach
     * instead of atomic approach.
     */
    public function get(array $names): array
    {
        $records = $this->blocking(function ($pipe) use ($names) {
            foreach ($names as $name) {
                $pipe->hmget('master:' . $name, ['name', 'pid', 'status', 'supervisors']);
            }
        });

        return collect($records)->map(function ($record) {
            $record = array_values($record);

            return ! $record[0] ? null : (object) [
                'name'        => $record[0],
                'pid'         => $record[1],
                'status'      => $record[2],
                'supervisors' => json_decode($record[3], true),
            ];
        })->filter()->all();
    }

    /**
     * {@inheritDoc}
     */
    public function update(MasterSupervisor $master): void
    {
        $supervisors = $master->supervisors->map->name->all();

        $this->blocking(function ($pipe) use ($master, $supervisors) {
            $pipe->hmset(
                'master:' . $master->name,
                [
                    'name'        => $master->name,
                    'pid'         => $master->pid(),
                    'status'      => $master->working ? 'running' : 'paused',
                    'supervisors' => json_encode($supervisors),
                ]
            );

            $pipe->zadd(
                'masters',
                CarbonImmutable::now()->getTimestamp(),
                $master->name
            );

            $pipe->expire('master:' . $master->name, 15);
        });
    }
}
