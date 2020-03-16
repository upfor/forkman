<?php

require dirname(__DIR__) . '/src/ForkMan.php';

use Upfor\ForkMan\ForkMan;

$fm = new ForkMan(2);
$fm->master(function (ForkMan $fm) {
    $fm->submit([1, 1000]);
    $fm->submit([1001, 2000]);
    $fm->submit([2001, 3000], function ($data, ForkMan $fm) {
        $fm->log($data);

        sleep(1);
        $fm->submit([3001, 4000]);
    });

    $fm->wait(3000);
})->slave(function ($params, ForkMan $fm) {
    // slave process's callback cannot print anything, print log please use $fm->log()
    $fm->log($params);
    return $params;
});
