package cn.tongji.cmis.dao;

import cn.tongji.cmis.domain.Task;

/**
 * 任务管理接口
 * Created by 626hp on 2017/10/24.
 */
public interface ITaskDAO {
    Task findById(long taskid);
}
