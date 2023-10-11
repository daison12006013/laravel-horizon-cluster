<?php

namespace Daison\LaravelHorizonCluster\Mod;

use Laravel\Horizon\Repositories\RedisTagRepository as Base;

/**
 * @author Daison Carino <daison12006013@gmail.com>
 */
class RedisTagRepository extends Base
{
    use PipelineToBlockingTrait;

    /**
     * Store the tags for the given job.
     *
     * @param  string  $id
     * @param  array  $tags
     * @return void
     */
    public function add($id, array $tags)
    {
        $this->blocking(function ($pipe) use ($id, $tags) {
            foreach ($tags as $tag) {
                $pipe->zadd($tag, str_replace(',', '.', microtime(true)), $id);
            }
        });
    }

    /**
     * Store the tags for the given job temporarily.
     *
     * @param  int  $minutes
     * @param  string  $id
     * @param  array  $tags
     * @return void
     */
    public function addTemporary($minutes, $id, array $tags)
    {
        $this->blocking(function ($pipe) use ($minutes, $id, $tags) {
            foreach ($tags as $tag) {
                $pipe->zadd($tag, str_replace(',', '.', microtime(true)), $id);

                $pipe->expire($tag, $minutes * 60);
            }
        });
    }

    /**
     * Remove the given job IDs from the given tag.
     *
     * @param  array|string  $tags
     * @param  array|string  $ids
     * @return void
     */
    public function forgetJobs($tags, $ids)
    {
        $this->blocking(function ($pipe) use ($tags, $ids) {
            foreach ((array) $tags as $tag) {
                foreach ((array) $ids as $id) {
                    $pipe->zrem($tag, $id);
                }
            }
        });
    }
}
