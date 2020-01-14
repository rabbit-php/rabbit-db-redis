<?php
declare(strict_types=1);

namespace rabbit\db\redis;

use rabbit\App;

/**
 * Class SentinelConnection
 * @package rabbit\db\redis
 */
class SentinelConnection
{
    /** @var string */
    public $hostname;
    /** @var string */
    public $masterName = 'mymaster';
    /** @var int */
    public $port = 26379;
    /** @var float */
    public $connectionTimeout;

    public $unixSocket;
    protected $_socket;
    /** @var int */
    public $retry = 3;


    /**
     * @return bool
     * @throws \Exception
     */
    protected function open(): bool
    {
        if (is_resource($this->_socket)) {
            return true;
        }
        $connection = ($this->unixSocket ?: $this->hostname . ':' . $this->port);
        App::info('Opening redis sentinel connection: ' . $connection, SentinelsManager::LOG_KEY);
        $this->_socket = @stream_socket_client($this->unixSocket ? 'unix://' . $this->unixSocket : 'tcp://' . $this->hostname . ':' . $this->port, $errorNumber, $errorDescription, $this->connectionTimeout ? $this->connectionTimeout : ini_get("default_socket_timeout"), STREAM_CLIENT_CONNECT);
        if ($this->_socket) {
            if ($this->connectionTimeout !== null) {
                stream_set_timeout($this->_socket, $timeout = (int)$this->connectionTimeout, (int)(($this->connectionTimeout - $timeout) * 1000000));
            }
            return true;
        } else {
            App::warning('Failed opening redis sentinel connection: ' . $connection, SentinelsManager::LOG_KEY);
            $this->_socket = false;
            return false;
        }
    }

    /**
     * @return bool
     */
    public function close(): bool
    {
        $res = fclose($this->_socket);
        $this->_socket = null;
        return $res;
    }

    /**
     * @return mixed|null
     * @throws \Exception
     */
    public function getMaster()
    {
        if ($this->open()) {
            return $this->executeCommand('sentinel', [
                'get-master-addr-by-name',
                $this->masterName
            ], $this->_socket);
        } else {
            return false;
        }
    }

    /**
     * @param string $name
     * @param array $params
     * @return array|bool|false|string|null
     * @throws \Exception
     */
    public function executeCommand(string $name, array $params)
    {
        $params = array_merge(explode(' ', $name), $params);
        $command = '*' . count($params) . "\r\n";
        foreach ($params as $arg) {
            $command .= '$' . mb_strlen($arg, '8bit') . "\r\n" . $arg . "\r\n";
        }

        $retry = $this->retry;
        while ($retry--) {
            if (fwrite($this->_socket, $command) === false) {
                $this->close();
                $this->open();
                continue;
            }
            return $this->parseResponse(implode(' ', $params));
        }
        throw new \Exception("Failed to read from socket.\nRedis command was: " . implode(' ', $params));
    }

    /**
     * @param string $command
     * @return array|bool|false|string|null
     * @throws \Exception
     */
    public function parseResponse(string $command)
    {
        if (($line = fgets($this->_socket)) === false) {
            throw new \Exception("Failed to read from socket.\nRedis command was: " . $command);
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
                throw new \Exception("Redis error: " . $line . "\nRedis command was: " . $command);
            case ':': // Integer reply
                // no cast to int as it is in the range of a signed 64 bit integer
                return $line;
            case '$': // Bulk replies
                if ($line == '-1') {
                    return null;
                }
                $length = $line + 2;
                $data = '';
                while ($length > 0) {
                    if (($block = fread($this->_socket, $length)) === false) {
                        throw new \Exception("Failed to read from socket.\nRedis command was: " . $command);
                    }
                    $data .= $block;
                    $length -= mb_strlen($block, '8bit');
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
                throw new \Exception('Received illegal data from redis: ' . $line . "\nRedis command was: " . $command);
        }
    }
}