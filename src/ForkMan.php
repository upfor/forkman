<?php

namespace Upfor\ForkMan;

/**
 * ForkMan
 */
class ForkMan
{

    /**
     * @var string Slave process identifier
     */
    public $slaveLabel = '-slave';

    /**
     * @var string Slave process command
     */
    public $slaveCmd;

    /**
     * @var string Process name
     */
    public $name;

    /**
     * @var int Process number
     */
    public $poolSize = 2;

    /**
     * @var string Slave command prefix
     */
    public $prefix = '';

    /**
     * @var array Pool of process
     */
    private $processPool = [];

    /**
     * @var bool Is a slave process
     */
    private $isSlave = false;

    /**
     * @var int The number of busy
     */
    private $idleCount = 0;

    /**
     * @var callable Master process handler
     */
    private $masterHandler;

    /**
     * @var callable Slave process handler
     */
    private $slaveHandler;

    /**
     * @param  int    $poolSize
     * @param  string $name
     */
    public function __construct($poolSize = 2, $name = '')
    {
        if (empty($name)) {
            $name = explode('\\', __CLASS__);
            $name = end($name);
        }

        $this->name = $name;
        $this->poolSize = $poolSize;

        if (!empty($_SERVER['argv']) && false !== array_search($this->slaveLabel, $_SERVER['argv'])) {
            $this->isSlave = true;
        }
    }

    /**
     * Execute only in master process
     *
     * @param  callable $masterHandler
     * @return $this
     */
    public function master(callable $masterHandler)
    {
        if (!$this->isSlave) {
            $this->masterHandler = $masterHandler;
            $this->createMaster($this->poolSize);
        }

        return $this;
    }

    /**
     * Create master handler
     *
     * @param  int $limit
     */
    private function createMaster($limit)
    {
        !$this->slaveCmd && $this->slaveCmd = $this->getCmd();

        for ($i = 0; $i < $limit; $i++) {
            $this->processPool[] = $this->createProcess();
        }

        @cli_set_process_title($this->name . ':' . 'master');

        if (is_callable($this->masterHandler)) {
            call_user_func($this->masterHandler, $this);
        }
    }

    /**
     * Get current command
     *
     * @return string
     */
    private function getCmd()
    {
        $prefix = empty($this->prefix) ? (isset($_SERVER['_']) ? $_SERVER['_'] : '/usr/bin/env php') : $this->prefix;
        $mixed = array_merge([$prefix, $_SERVER['PHP_SELF']], $_SERVER['argv']);

        return implode(' ', array_unique($mixed));
    }

    /**
     * Create process
     *
     * @return array
     */
    private function createProcess()
    {
        $desc = [
            ['pipe', 'r'], // std input
            ['pipe', 'w'], // std output
            ['pipe', 'w'], // std error
        ];
        $res = proc_open($this->slaveCmd . ' ' . $this->slaveLabel, $desc, $pipes, getcwd());
        $pid = proc_get_status($res)['pid'];
        $process = [
            'res' => $res,
            'pipes' => $pipes,
            'status' => true, // true:idle
            'pid' => $pid,
            'callback' => null, // call when the slave process finished
        ];

        // non-blocking
        stream_set_blocking($pipes[1], 0);
        stream_set_blocking($pipes[2], 0);

        $this->log('start ' . $pid);
        return $process;
    }

    /**
     * Logger
     *
     * @param  string $str
     */
    public function log($str)
    {
        if (is_array($str)) {
            $str = json_encode($str, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        }
        $args = func_get_args();
        $line = count($args) > 1 ? call_user_func_array('sprintf', $args) : $str;

        $line = str_pad(date('Y-m-d H:i:s') . ' [' . ($this->isSlave ? 'slave' : 'master') . ':' . getmypid() . '] ', 16, ' ', STR_PAD_RIGHT) . $line . "\n";

        error_log($line, 3, $this->isSlave ? 'php://stderr' : 'php://stdout');
    }

    /**
     * Execute only in slave process
     *
     * @param  callable $slaveHandler
     * @return $this
     */
    public function slave(callable $slaveHandler)
    {
        if ($this->isSlave) {
            $this->slaveHandler = $slaveHandler;
            $this->createSlave();
        }

        return $this;
    }

    /**
     * Create slave handler
     */
    private function createSlave()
    {
        @cli_set_process_title($this->name . ':' . 'slave');

        while (true) {
            // listen input from master
            $fp = @fopen('php://stdin', 'r');
            $recv = @fread($fp, 8); // read content length
            $size = intval(rtrim($recv));
            $data = @fread($fp, $size);
            @fclose($fp);

            if (!empty($data)) {
                if (is_callable($this->slaveHandler)) {
                    $data = json_decode($data, true);
                    $resp = call_user_func($this->slaveHandler, $data, $this);
                    echo json_encode($resp);
                }
            } else {
                usleep(100000);
            }
        }
    }

    /**
     * Master submit task to slave
     *
     * @param  mixed    $data
     * @param  callable $callback
     */
    public function submit($data, $callback = null)
    {
        if (!$this->isSlave) {
            $process = &$this->getAvailableProcess();
            $process['callback'] = $callback;
            $data = json_encode($data);
            $length = strlen($data);
            $length = str_pad($length . '', 8, ' ', STR_PAD_RIGHT);

            // send to slave process, with length and content
            fwrite($process['pipes'][0], $length . $data);
        }
    }

    /**
     * Get available process
     *
     * @return mixed
     */
    private function &getAvailableProcess()
    {
        while (true) {
            $index = $this->check();
            if (isset($this->processPool[$index])) {
                $this->processPool[$index]['status'] = false;
                $this->idleCount++;
                return $this->processPool[$index];
            }
            // sleep 50 ms
            usleep(50000);
        }

        return null;
    }

    /**
     * Check process
     *
     * @return int
     */
    private function check()
    {
        $index = -1;
        foreach ($this->processPool as $key => &$process) {
            $this->checkProcessAlive($process);
            if (!$process['status']) {
                echo stream_get_contents($process['pipes'][2]);
                $result = stream_get_contents($process['pipes'][1]);
                if (!empty($result)) {
                    $process['status'] = true;
                    $this->idleCount--;

                    if (is_callable($process['callback'])) {
                        $process['callback'](json_decode($result, true));
                    }
                }
            }
            if ($process['status'] && $index < 0) {
                $index = $key;
            }
        }
        return $index;
    }

    /**
     * Check a process is alive
     *
     * @param  array $process
     */
    private function checkProcessAlive(&$process)
    {
        $status = proc_get_status($process['res']);
        if (!$status['running']) {
            echo stream_get_contents($process['pipes'][2]);

            $this->killProcess($process);
            $this->log('close ' . $process['pid']);
            if (!$process['status']) {
                $this->idleCount--;
            }
            $process = $this->createProcess();
        }
    }

    /**
     * Kill process
     *
     * @param  array $process
     * @return bool
     */
    private function killProcess($process)
    {
        if (function_exists('proc_terminate')) {
            return @proc_terminate($process['res']);
        } elseif (function_exists('posix_kill')) {
            return @posix_kill($process['pid'], 9);
        }

        return false;
    }

    /**
     * Loop condition
     *
     * @param  int $sleep (unit: ms)
     * @return bool
     */
    public function loop($sleep = 0)
    {
        if (!$this->isSlave) {
            if ($sleep > 0) {
                usleep($sleep * 1000);
            }

            $this->check();
            return true;
        }

        return false;
    }

    /**
     * Wait all process idled or timeout
     *
     * @param  int $timeout (unit: ms)
     */
    public function wait($timeout = 0)
    {
        $start = microtime(true);

        while (true) {
            $this->check();
            $interval = (microtime(true) - $start) * 1000;

            // timeout or all processes idle
            $outed = $timeout > 0 && $interval >= $timeout;
            if ($outed || $this->idleCount <= 0) {
                $killStatus = $this->killAllProcess();
                if ($killStatus) {
                    $this->log('all slave processes exited(' . ($outed ? 'timeout' : 'idle') . ')');
                    return;
                }
            }

            usleep(10000);
        }
    }

    /**
     * Kill all
     *
     * @return bool
     */
    private function killAllProcess()
    {
        $killStatus = true;
        foreach ($this->processPool as &$process) {
            $status = $this->killProcess($process);
            if ($status) {
                $this->log('close success: ' . $process['pid']);
                !$process['status'] && $this->idleCount--;
            } else {
                $this->log('close failed: ' . $process['pid']);
                $killStatus = false;
            }
        }

        return $killStatus;
    }

}
