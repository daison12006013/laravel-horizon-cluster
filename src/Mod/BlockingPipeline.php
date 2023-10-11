<?php

declare(strict_types=1);

namespace Daison\LaravelHorizonCluster\Mod;

/**
 * @author Daison Carino <daison12006013@gmail.com>
 */
class BlockingPipeline
{
    protected array $processed = [];

    public function __construct(protected $connection)
    {
    }

    public function processed(): array
    {
        return $this->processed;
    }

    public function __call(string $name, array $args): mixed
    {
        $process = $this->connection->{$name}(...$args);

        $this->processed[] = $process;

        return $process;
    }
}
