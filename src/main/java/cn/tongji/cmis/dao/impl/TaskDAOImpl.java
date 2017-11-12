package cn.tongji.cmis.dao.impl;

import cn.tongji.cmis.dao.ITaskDAO;
import cn.tongji.cmis.domain.Task;
import cn.tongji.cmis.jdbc.JDBCHelper;

import java.sql.ResultSet;

/**
 * Created by 626hp on 2017/10/24.
 */
public class TaskDAOImpl implements ITaskDAO {

    @Override
    public Task findById(long taskid) {
        final Task task=new Task();

        String sql="select * from task where task_id=?";
        Object[] params=new Object[]{taskid};

        JDBCHelper jdbcHelper=JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if(rs.next())
                {
                    long taskid=rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);

                    task.setTaskid(taskid);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });

        return task;
    }
}
