<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Psr\SimpleCache\CacheInterface;
use Rabbit\Cache\AbstractCache;

/**
 * Class Cache
 * @package rabbit\db\redis
 */
class Cache extends AbstractCache implements CacheInterface
{
    private ?Redis $client;

    public function __construct()
    {
        parent::__construct();
        $this->client = getDI('redis')->get();
    }

    public function get(string $key, mixed $default = null): mixed
    {
        $key = $this->buildKey($key);
        $result = $this->client->executeCommand('GET', [$key]);
        if ($result === false || $result === null) {
            return $default;
        }

        return $result;
    }

    public function set(string $key, mixed $value, null|int|\DateInterval $ttl = null): bool
    {
        $key = $this->buildKey($key);
        if ($ttl === null) {
            return (bool)$this->client->executeCommand('SET', [$key, $value]);
        } else {
            return (bool)$this->client->executeCommand('SET', [$key, $value, 'EX', $ttl]);
        }
    }

    public function delete(string $key): bool
    {
        $this->buildKey($key);
        return (bool)$this->client->executeCommand('DEL', [$key]);
    }

    public function clear(): bool
    {
        return $this->client->executeCommand('FLUSHDB');
    }

    public function getMultiple(iterable $keys, mixed $default = null): iterable
    {
        $newKeys = [];
        foreach ($keys as $key) {
            $newKeys[] = $this->buildKey($key);
        }
        $response = $this->client->executeCommand('MGET', $newKeys);
        $result = [];
        $i = 0;
        foreach ($keys as $key) {
            $result[$key] = $response[$i++];
        }

        return $result;
    }

    public function setMultiple(iterable $values, null|int|\DateInterval $ttl = null): bool
    {
        $args = [];
        foreach ($values as $key => $value) {
            $args[] = $this->buildKey($key);
            $args[] = $value;
        }

        $failedKeys = [];
        if ($ttl == 0) {
            $this->client->executeCommand('MSET', $args);
        } else {
            $ttl = (int)($ttl * 1000);
            $this->client->executeCommand('MULTI');
            $this->client->executeCommand('MSET', $args);
            $index = [];
            foreach ($values as $key => $value) {
                $this->client->executeCommand('PEXPIRE', [$key, $ttl]);
                $index[] = $key;
            }
            $result = $this->client->executeCommand('EXEC');
            array_shift($result);
            foreach ($result as $i => $r) {
                if ($r != 1) {
                    $failedKeys[] = $index[$i];
                }
            }
        }
        return count($failedKeys) === 0;
    }

    public function deleteMultiple(iterable $keys): bool
    {
        $newKeys = [];
        foreach ($keys as $key) {
            $newKeys[] = $this->buildKey($key);
        }
        return (bool)$this->client->executeCommand('DEL', $newKeys);
    }

    public function has(string $key): bool
    {
        return (bool)$this->client->executeCommand('EXISTS', [$this->buildKey($key)]);
    }
}
