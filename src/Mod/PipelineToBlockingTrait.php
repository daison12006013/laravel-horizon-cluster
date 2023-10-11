<?php

declare(strict_types=1);

namespace Daison\LaravelHorizonCluster\Mod;

use Closure;

/**
 * @author Daison Carino <daison12006013@gmail.com>
 */
trait PipelineToBlockingTrait
{
    /**
     * Instead of using pipeline, we will do a blocking.
     *
     * @param Closure $closure
     * @return mixed
     */
    public function blocking(Closure $closure): mixed
    {
        $pipe              = new BlockingPipeline($this->connection());
        $closureResponse   = $closure($pipe);
        $pipelineProcessed = $pipe->processed();

        return !empty($closureResponse) ? $closureResponse : $pipelineProcessed;
    }
}
