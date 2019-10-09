A Laravel redis wrapper library for phpredis
=======================

Make PhpRedis match Predis method signature

## Requirement

- PHP 7.1+
- PhpRedis
- Laravel 5.4

## Installation

Install via composer
```
composer require taptap/laravel-phpredis:^1.0
```

Add provider to `config/app.php` and comment out `Illuminate\Redis\RedisServiceProvider`
```php
'providers' => [
    ...
    // Illuminate\Redis\RedisServiceProvider::class,
    \TapTap\LaravelRedis\RedisServiceProvider::class,
    ...
]
```

That's all!
