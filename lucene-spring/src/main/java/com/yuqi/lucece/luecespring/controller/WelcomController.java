package com.yuqi.lucece.luecespring.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author yuqi
 * Time 7/9/19
 **/

@RestController
public class WelcomController {
    @RequestMapping("/")
    public String index() {
        return "Welcome to query system!";
    }
}
