目标：通过Occlum.json来动态配置mlsdisk中的关键数据结构缓存配置，
举个例子：位于core/src/layers/5-disk/sworndisk.rs 的DATA_BUF_CAP，可以通过如下Occlum.json中的data_buf_cap来在启动sworndisk时动态调整它的大小
```json
    {
      "target": "/root",
      "type":"ext2",
      "options": {
        "disk_size": "10GB",
        "data_buf_cap":1024
      }
    }
```
实现目标的方法步骤：
1）目前occlum已经实现了动态配置disk_size，你需要梳理清楚Occlum.json中的参数是如何一步一步传递到sworndisk并完成初始化的，你需要重点关注occlum的pal blk fs gen_internal_conf这几个源码目录
2）根据步骤1）获得的代码调用链路关系，制定方案来实现通过Occlum.json来配置sworndisk.rs 的DATA_BUF_CAP
3) 有一个注意事项， "disk_size": "10GB",传递的是字符串“10GB”并且是磁盘大小，data_buf_cap传递的是一个数值
4) 你要保持和原项目一致的编码规范和风格