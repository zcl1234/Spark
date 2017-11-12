package cn.tongji.cmis.dao;


import cn.tongji.cmis.domain.SessionRandomExtract;

/**
 * session随机抽取模块DAO接口
 * @author Administrator
 *
 */
public interface ISessionRandomExtractDAO{

	/**
	 * 插入session随机抽取
	 */
	void insert(SessionRandomExtract sessionRandomExtract);
	
}
