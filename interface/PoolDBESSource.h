// system include files
#include <memory>
#include <string>
#include <map>

// user include files
#include "FWCore/Framework/interface/DataProxyProvider.h"
#include "FWCore/Framework/interface/EventSetupRecordIntervalFinder.h"
#include "DataSvc/Ref.h"
namespace pool{
  class IFileCatalog;
  class IDataSvc;
}
namespace edm{
  class ParameterSet;
}
namespace cond{
  class IOV;
}
class PoolDBESSource : public edm::eventsetup::DataProxyProvider, 
		       public edm::EventSetupRecordIntervalFinder
{
public:
  PoolDBESSource( const edm::ParameterSet& );
  ~PoolDBESSource();
  
protected:
  virtual void setIntervalFor(const edm::eventsetup::EventSetupRecordKey&,
			      const edm::IOVSyncValue& , 
			      edm::ValidityInterval&) ;
  virtual void registerProxies(const edm::eventsetup::EventSetupRecordKey& iRecordKey, KeyedProxies& aProxyList) ;
  virtual void newInterval(const edm::eventsetup::EventSetupRecordKey& iRecordType, const edm::ValidityInterval& iInterval) ;
  
private:
  // ----------member data ---------------------------
  typedef std::multimap<std::string, std::string> RecordToTypes;
  RecordToTypes m_recordToTypes; //should be static?
  typedef std::string proxyName;
  typedef std::string tokenType;
  typedef std::map<proxyName, tokenType > ProxyToToken;
  ProxyToToken m_proxyToToken;
  typedef ProxyToToken::iterator pProxyToToken;
  std::string m_con;
  typedef std::map<std::string, pool::Ref<cond::IOV> > RecordToIOV;
  RecordToIOV m_recordToIOV;
  std::string m_timetype;
  std::auto_ptr<pool::IFileCatalog> m_cat;
  pool::IDataSvc* m_svc;
private:
  void initPool();
  void closePool();
  bool initIOV( const std::vector< std::pair<std::string,std::string> >& record2tag );
};

