[![Packagist Downloads](https://img.shields.io/packagist/dt/daison/laravel-horizon-cluster)](https://packagist.org/packages/daison/laravel-horizon-cluster/stats)

![seeking's laravel horizon](sa_prod_screenshot.png)

# Laravel Horizon - Cluster Support

This package extends Laravel Horizon with support for a Redis cluster. Based on my testing, it should also be compatible with AWS Elastic Load Balancer.

## Installation

```bash
composer require daison/laravel-horizon-cluster
```

After installing this package, now publish the assets using `horizon:install`

```bash
php artisan horizon:install
```

### Remove the auto discover

Modify your original laravel's `composer.json` and add this

```json
{
    "extra": {
        "laravel": {
            "dont-discover": [
                "laravel/horizon"
            ]
        }
    }
}
```

### Use the modified horizon

Modify your `config/app.php`

```php
return [
    'providers' => [
        // ...

        Daison\LaravelHorizonCluster\AppServiceProvider::class,
        App\Providers\HorizonServiceProvider::class,
    ],
];
```

## config/database.php

Usually your laravel `config/database.php` should look like this.

```php
return [
    'redis' => [
        'client' => 'predis',

        'clusters' => [
            'default' => [
                [
                    'host'     => env('REDIS_HOST', '127.0.0.1'),
                    'port'     => env('REDIS_PORT', '6379'),
                    'password' => env('REDIS_PASSWORD', null),
                    'database' => 0,
                ],
            ],
            // ...
        ],

        'options' => [
            'cluster' => 'redis',
        ],
    ],
];
```

## config/horizon.php

Make sure your horizon will have this kind of config or similar.

```php
return [
    'use' => 'clusters.default',

    // ...

    'defaults' => [
        'worker' => [
            'connection' => 'redis',
            'balance'    => env('HORIZON_QUEUE_WORKER_BALANCE', false),
            'timeout'    => env('HORIZON_QUEUE_WORKER_TIMEOUT', 10),
            'sleep'      => env('HORIZON_QUEUE_WORKER_SLEEP', 3),
            'maxTries'   => env('HORIZON_QUEUE_WORKER_MAXTRIES', 3),
        ],
    ],

    'environments' => [
        env('APP_ENV') => [
            'worker' => [
                'connection' => 'redis',
                'queue'      => [
                    '{redis-high}',
                    '{redis}',
                    '{redis-low}',
                ],
                'memory'       => env('HORIZON_QUEUE_WORKER_MEMORY', 128),
                'minProcesses' => env('HORIZON_QUEUE_WORKER_MIN_PROCESSES', 1),
                'maxProcesses' => env('HORIZON_QUEUE_WORKER_MAX_PROCESSES', 3),
            ],
        ],
    ],
];
```

## config/queue.php

Make sure your `'redis'` is wrapped with curly brace

```
// ...

'redis-low' => [
    // ...
    'queue' => env('REDIS_QUEUE', '{redis-low}'),
    // ...
],

'redis' => [
    // ...
    'queue' => env('REDIS_QUEUE', '{redis}'),
    // ...
],

'redis-high' => [
    // ...
    'queue' => env('REDIS_QUEUE', '{redis-high}'),
    // ...
],
```

Enjoy using Laravel Horizon with Cluster support!
