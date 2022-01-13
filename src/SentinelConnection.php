<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Co\Client;
use Throwable;
use Rabbit\Base\App;

/**
 * Class SentinelConnection
 * @package Rabbit\DB\Redis
 */
class SentinelConnection
{
    public string $hostname = 'localhost';

    public string $masterName = 'mymaster';

    public int $port = 26379;

    public ?float $connectionTimeout;

    public ?string $unixSocket = null;

    protected ?Client $_socket = null;

    public int $retry = 3;

    protected function open(): bool
    {
        if ($this->_socket !== null && $this->_socket->connected) {
            return true;
        }
        $connection = ($this->unixSocket ?: $this->hostname . ':' . $this->port);
        App::info('Opening redis sentinel connection: ' . $connection, SentinelsManager::LOG_KEY);
        $this->_socket = new Client(SWOOLE_SOCK_TCP);
        $retries = $this->retry;
        $retries = $retries > 0 ? $retries : 1;
        while ($retries--) {
            if ($this->_socket->connect($this->hostname, $this->port, $this->connectionTimeout ?? 3) === false) {
                App::warning('Failed opening redis sentinel connection: ' . $connection, SentinelsManager::LOG_KEY);
                continue;
            }
            $this->_socket->set([
                'open_eof_check' => true,
                'package_eof' => PHP_EOL,
            ]);
            return true;
        }
        return false;
    }

    public function close(): bool
    {
        $res = (bool)$this->_socket->close();
        $this->_socket = null;
        return $res;
    }

    public function getMaster(): array|bool|string|null
    {
        if ($this->open()) {
            return $this->executeCommand(
                'sentinel',
                [
                    'master',
                    $this->masterName
                ]
            );
        } else {
            return false;
        }
    }

    public function getSlave(): array|bool|string|null
    {
        if ($this->open()) {
            return $this->executeCommand('sentinel', [
                'slaves',
                $this->masterName
            ]);
        } else {
            return false;
        }
    }

    public function executeCommand(string $name, array $params): array|bool|string|null
    {
        if (!$this->_socket->connected) {
            $this->open();
        }
        $params = [...explode(' ', $name), ...$params];
        $command = '*' . count($params) . "\r\n";
        foreach ($params as $arg) {
            $command .= '$' . mb_strlen($arg, '8bit') . "\r\n" . $arg . "\r\n";
        }

        $retries = $this->retry;
        $retries = $retries > 0 ? $retries : 1;
        while ($retries--) {
            try {
                $written = $this->_socket->send($command);
                if ($written === false) {
                    throw new SocketException("Failed to write to socket.\nRedis command was: " . $command);
                }
                if ($written !== ($len = mb_strlen($command, '8bit'))) {
                    throw new SocketException("Failed to write to socket. $written of $len bytes written.\nRedis command was: " . $command);
                }

                return $this->parseResponse(implode(' ', $params));
            } catch (Throwable $exception) {
                $this->close();
                $this->open();
            }
        }
        throw new SocketException("Failed to read from socket.\nRedis command was: " . implode(' ', $params));
    }

    public function parseResponse(string $command): array|bool|string|null
    {
        if (($line = $this->_socket->recv()) === false) {
            throw new SocketException("Failed to read from socket.\nRedis command was: " . $command);
        }
        $type = $line[0];
        $line = mb_substr($line, 1, -2, '8bit');
        switch ($type) {
            case '+': // Status reply
                if ($line === 'OK' || $line === 'PONG') {
                    return true;
                } else {
                    return $line;
                }
            case '-': // Error reply
                throw new SocketException("Redis error: " . $line . "\nRedis command was: " . $command);
            case ':': // Integer reply
                // no cast to int as it is in the range of a signed 64 bit integer
                return $line;
            case '$': // Bulk replies
                if ($line == '-1') {
                    return null;
                }
                $length = $line + 2;
                $data = '';
                if ($length > 0) {
                    if (($data = $this->_socket->recv()) === false) {
                        throw new SocketException("Failed to read from socket.\nRedis command was: " . $command);
                    }
                }

                return mb_substr($data, 0, -2, '8bit');
            case '*': // Multi-bulk replies
                $count = (int)$line;
                $data = [];
                for ($i = 0; $i < $count; $i++) {
                    $data[] = $this->parseResponse($command);
                }

                return $data;
            default:
                throw new SocketException('Received illegal data from redis: ' . $line . "\nRedis command was: " . $command);
        }
    }
}
