<?php

namespace Daison\LaravelHorizonCluster\Mod;

use Laravel\Horizon\RedisHorizonCommandQueue as Base;

/**
 * @author Daison Carino <daison12006013@gmail.com>
 */
class RedisHorizonCommandQueue extends Base
{
    use PipelineToBlockingTrait;

    /**
     * Get the pending commands for a given queue name.
     *
     * @param  string  $name
     * @return array
     */
    public function pending($name): array
    {
        $length = $this->connection()->llen('commands:' . $name);

        if ($length < 1) {
            return [];
        }

        $results = $this->blocking(function ($pipe) use ($name, $length) {
            return [
                $pipe->lrange('commands:' . $name, 0, $length - 1),
                $pipe->ltrim('commands:' . $name, $length, -1),
            ];
        });

        return collect($results[0])->map(function ($result) {
            return (object) json_decode($result, true);
        })->all();
    }
}
