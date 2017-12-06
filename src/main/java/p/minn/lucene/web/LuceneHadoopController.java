package p.minn.lucene.web;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.SessionAttributes;

import p.minn.common.annotation.MyParam;
import p.minn.common.exception.WebPrivilegeException;
import p.minn.lucene.service.LuceneHadoopService;
import p.minn.privilege.utils.Constant;

/**
 * 
 * @author minn 
 * @QQ:3942986006
 * 
 */
@Controller
@RequestMapping("/lucene")
@SessionAttributes(Constant.LOGINUSER)
public class LuceneHadoopController {

	@Autowired
	private LuceneHadoopService luceneHadoopService;
	
	@RequestMapping(params="method=query")
	public Object query(@RequestParam("messageBody") String messageBody,@MyParam("language") String lang){
		Object entity = null;
		try {
			entity=luceneHadoopService.query(messageBody, lang);
		 } catch (Exception e) {
				entity = new WebPrivilegeException(e.getMessage());
		 }
		return entity;
	}
	
	
	@RequestMapping(params="method=add")
	public Object add(@RequestParam("messageBody") String messageBody,@MyParam("language") String lang){
		Object entity = null;
		try {
		     entity=luceneHadoopService.add(messageBody,lang);
		 } catch (Exception e) {
			 e.printStackTrace();
				entity = new WebPrivilegeException(e.getMessage());
		 }
		return entity;
	}
	
	
}
