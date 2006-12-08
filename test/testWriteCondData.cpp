#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Time.h"
#include "CondCore/DBCommon/interface/SessionConfiguration.h"
#include "CondCore/DBCommon/interface/PoolStorageManager.h"
#include "CondCore/DBCommon/interface/RelationalStorageManager.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/ConnectMode.h"
#include "CondCore/DBCommon/interface/MessageLevel.h"
#include "CondCore/IOVService/interface/IOVService.h"
#include "CondCore/IOVService/interface/IOVEditor.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "CondCore/DBCommon/interface/ConnectMode.h"
#include "CondCore/DBCommon/interface/Ref.h"
//#include "CondFormats/EcalObjects/interface/EcalPedestals.h"
#include "CondFormats/Calibration/interface/Pedestals.h"
int main(){
  try{
    cond::DBSession* session=new cond::DBSession("sqlite_file:test.db");
    session->sessionConfiguration().setMessageLevel(cond::Error);
    session->open(true);
    cond::PoolStorageManager& pooldb=session->poolStorageManager("file:mycatalog.xml");
    pooldb.connect();
    cond::IOVService iovmanager(pooldb);
    cond::IOVEditor* ioveditor=iovmanager.newIOVEditor();
    pooldb.startTransaction(false);
    for(unsigned int i=0; i<3; ++i){ //inserting 3 payloads
      Pedestals* myped=new Pedestals;
      for(int ichannel=1; ichannel<=5; ++ichannel){
	Pedestals::Item item;
        item.m_mean=1.11*ichannel+i;
        item.m_variance=1.12*ichannel+i*2;
        myped->m_pedestals.push_back(item);
      }
      cond::Ref<Pedestals> myref(pooldb,myped);
      myref.markWrite("PedestalsRcd");
      std::string payloadToken=myref.token();
      ioveditor->insert(cond::Time_t(2+2*i),payloadToken);
    }
    //last one
    Pedestals* myped=new Pedestals;
    for(int ichannel=1; ichannel<=5; ++ichannel){
      Pedestals::Item item;
      item.m_mean=3.11*ichannel;
      item.m_variance=5.12*ichannel;
      myped->m_pedestals.push_back(item);
    }
    cond::Ref<Pedestals> myref(pooldb,myped);
    myref.markWrite("PedestalsRcd");
    std::string payloadToken=myref.token();
    ioveditor->insert(9999,payloadToken);
    std::string iovtoken=ioveditor->token();
    std::cout<<"iov token "<<iovtoken<<std::endl;
    pooldb.commit();
    pooldb.disconnect();
    delete ioveditor;
    cond::RelationalStorageManager& coraldb=session->relationalStorageManager();
    coraldb.connect(cond::ReadWriteCreate);
    coraldb.startTransaction(false);
    cond::MetaData metadata(coraldb);
    metadata.addMapping("mytest",iovtoken);
    coraldb.commit();
    coraldb.disconnect();
    session->close();
    delete session;
    session=0;
  }catch(const cond::Exception& er){
    std::cout<<"error "<<er.what()<<std::endl;
  }catch(const std::exception& er){
    std::cout<<"std error "<<er.what()<<std::endl;
  }
}