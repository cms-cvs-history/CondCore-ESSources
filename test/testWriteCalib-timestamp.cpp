#include "CondCore/DBCommon/interface/DBWriter.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/ServiceLoader.h"
#include "CondCore/DBCommon/interface/ConnectMode.h"
#include "CondCore/DBCommon/interface/MessageLevel.h"
#include "CondFormats/EcalObjects/interface/EcalPedestals.h"
#include "CondFormats/Calibration/interface/Pedestals.h"
#include "CondCore/IOVService/interface/IOV.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "FWCore/Framework/interface/IOVSyncValue.h"
#include <string>
#include <map>
#include <iostream>
int main(){
  cond::ServiceLoader* loader=new cond::ServiceLoader;
  ::putenv("CORAL_AUTH_USER=me");
  ::putenv("CORAL_AUTH_PASSWORD=mypass");
  loader->loadAuthenticationService( cond::Env );
  loader->loadMessageService( cond::Error );
  try{
    cond::DBSession* session1=new cond::DBSession(std::string("sqlite_file:test.db"));
    session1->setCatalog("file:mycatalog.xml");
    session1->connect(cond::ReadWriteCreate);
    cond::DBWriter pwriter(*session1, "Pedestals");
    cond::DBWriter iovwriter(*session1, "IOV");
    session1->startUpdateTransaction();
    cond::IOV* pediov=new cond::IOV;
    unsigned long long infTime=edm::IOVSyncValue::endOfTime().time().value();
    for( int i=0; i<3; ++i){  //writing 4 peds
      unsigned long long currentTime=(i+1)*10000000; //10milsec
      Pedestals* myped=new Pedestals;
      for(int ichannel=1; ichannel<=5; ++ichannel){
	Pedestals::Item item;
        item.m_mean=1.11*ichannel;
        item.m_variance=1.12*ichannel;
        myped->m_pedestals.push_back(item);
      }
      std::string mytok=pwriter.markWrite<Pedestals>(myped);
      if(i<2){
	pediov->iov.insert(std::make_pair(currentTime,mytok)); 
      }else{ //the last one valid forever
	pediov->iov.insert(std::make_pair(infTime,mytok));
      }
    }
    std::string pediovToken=iovwriter.markWrite<cond::IOV>(pediov);  
    session1->commit();//commit all in one
    session1->disconnect();
    delete session1;
    cond::MetaData metadata_svc("sqlite_file:test.db",*loader);
    metadata_svc.connect();
    metadata_svc.addMapping("mytest", pediovToken);
    metadata_svc.disconnect();
  }catch(const cond::Exception& er){
    std::cout<<er.what()<<std::endl;
    delete loader;
    exit(-1);
  }catch(...){
    std::cout<<"Funny error"<<std::endl;
    delete loader;
    exit(-1);
  }
  
  try{
    cond::DBSession* session2=new cond::DBSession(std::string("sqlite_file:ecalcalib.db"));
    session2->setCatalog("file:mycatalog.xml");
    session2->connect(cond::ReadWriteCreate);
    cond::DBWriter epedwriter(*session2,"EcalPedestals");
    cond::DBWriter eiovwriter(*session2, "IOV");
    cond::IOV* epediov=new cond::IOV;
    session2->startUpdateTransaction();
    for(int i=0; i<3; ++i){ //writing 3 ecalped
      unsigned long long currentTime=(i+1)*5000000; //5milsec
      EcalPedestals* eped=new EcalPedestals;
      for(int ichannel=1658; ichannel<=1668; ++ichannel){
	EcalPedestals::Item item;
	item.mean_x1 =(0.91*i)+ichannel;
	item.rms_x1  =(0.17*i)+ichannel;
	item.mean_x6 =(0.52*i)+ichannel;
	item.rms_x6  =(0.03*i)+ichannel;
	item.mean_x12=(0.16*i)+ichannel;
	item.rms_x12 =(0.05*i)+ichannel;
	eped->m_pedestals.insert(std::make_pair(ichannel,item));
      }
      std::string epedtok=epedwriter.markWrite<EcalPedestals>(eped);
      //pool::Ref takes the ownership 
      std::cout<<"ecalped token "<<epedtok<<std::endl;
      //assign IOV
      if(i<2){
	epediov->iov.insert(std::make_pair(currentTime,epedtok));
      }else{
	//the last one valid forever
	epediov->iov.insert(std::make_pair(edm::IOVSyncValue::endOfTime().time().value(),epedtok));
      }
    }
    std::string epediovtok=eiovwriter.markWrite<cond::IOV>(epediov); 
    std::cout<<"ecalped iov token "<<epediovtok<<std::endl; 
    session2->commit();//commit all in one
    session2->disconnect();
    delete session2;
    cond::MetaData ecalmetadata_svc(std::string("sqlite_file:ecalcalib.db"),*loader);
    ecalmetadata_svc.connect();
    ecalmetadata_svc.addMapping("ecaltest", epediovtok);
    ecalmetadata_svc.disconnect();
  }catch(const cond::Exception& er){
    std::cout<<er.what()<<std::endl;
    delete loader;
    exit(-1);
  }catch(...){
    std::cout<<"Funny error"<<std::endl;
    delete loader;
    exit(-1);
  }
  delete loader;
}
