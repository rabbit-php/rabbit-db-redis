<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Co\Client;
use Rabbit\Base\App;
use Throwable;

/**
 * Class SentinelConnection
 * @package Rabbit\DB\Redis
 */
class SentinelConnection
{
    /** @var string */
    public string $hostname = 'localhost';
    /** @var string */
    public string $masterName = 'mymaster';
    /** @var int */
    public int $port = 26379;
    /** @var float */
    public ?float $connectionTimeout;

    public ?string $unixSocket = null;
    /** @var Client */
    protected ?Client $_socket = null;
    /** @var int */
    public int $retry = 3;


    /**
     * @return bool
     * @throws Throwable
     */
    protected function open(): bool
    {
        if ($this->_socket !== null && $this->_socket->connected) {
            return true;
        }
        $connection = ($this->unixSocket ?: $this->hostname . ':' . $this->port);
        App::info('Opening redis sentinel connection: ' . $connection, SentinelsManager::LOG_KEY);
        $this->_socket = new Client(SWOOLE_SOCK_TCP);
        $retrys = $this->retry;
        $retrys = $retrys > 0 ? $retrys : 1;
        while ($retrys--) {
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

    /**
     * @return bool
     */
    public function close(): bool
    {
        $res = (bool)$this->_socket->close();
        $this->_socket = null;
        return $res;
    }

    /**
     * @return array|bool|false|string|null
     * @throws Throwable
     */
    public function getMaster()
    {
        if ($this->open()) {
            return $this->executeCommand('sentinel',
                [
                    'master',
                    $this->masterName
                ]);
        } else {
            return false;
        }
    }

    /**
     * @return array|bool|false|string|null
     * @throws Throwable
     */
    public function getSlave()
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

    /**
     * @param string $name
     * @param array $params
     * @return array|bool|false|string|null
     * @throws Throwable
     */
    public function executeCommand(string $name, array $params)
    {
        if (!$this->_socket->connected) {
            $this->open();
        }
        $params = array_merge(explode(' ', $name), $params);
        $command = '*' . count($params) . "\r\n";
        foreach ($params as $arg) {
            $command .= '$' . mb_strlen($arg, '8bit') . "\r\n" . $arg . "\r\n";
        }

        $retrys = $this->retry;
        $retrys = $retrys > 0 ? $retrys : 1;
        while ($retrys--) {
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

    /**
     * @param string $command
     * @return array|bool|false|string|null
     * @throws SocketException
     */
    public function parseResponse(string $command)
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