package mz.project.sparkboot.controllers;

import mz.project.sparkboot.services.WordService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class WordController {

    @Autowired
    WordService ws;

    @GetMapping("topX/{number}/{filename}")
    public Map<String, Integer> getPopularWords(@PathVariable int number, @PathVariable String filename){
        return ws.getWordsMap(number, filename);
    }
}
