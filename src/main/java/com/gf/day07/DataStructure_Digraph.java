package com.gf.day07;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataStructure_Digraph {
    public static void main(String[] args) {
        // {
        //    "source": ["map[0]", "map[1]"],
        //    "map[0]": ["filter"],
        //    "map[1]": ["filter"],
        //    "filter": ["print"]
        // }
        Map<String, List<String>> graph = new HashMap<>();
        List<String> sourceNeighbours = new ArrayList<>();
        sourceNeighbours.add("map[0]");
        sourceNeighbours.add("map[1]");
        List<String> map0Neighbours = new ArrayList<>();
        map0Neighbours.add("filter");
        map0Neighbours.add("map[1]");
        List<String> map1Neighbours = new ArrayList<>();
        map1Neighbours.add("filter");
        List<String> filterNeighbours = new ArrayList<>();
        filterNeighbours.add("print");

        graph.put("source",sourceNeighbours);
        graph.put("map[0]",map0Neighbours);
        graph.put("map[1]",map1Neighbours);
        graph.put("filter",filterNeighbours);
        topologySort(graph,"source","source");

    }
    public static void topologySort(Map<String,List<String>> graph,String node,String result){
        if (!graph.containsKey(node)) {
            System.out.println(result);
        }else {
            for (String n : graph.get(node)) {
                topologySort(graph,n,result+"==>"+n);
            }
        }
    }
}
