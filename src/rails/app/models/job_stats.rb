class JobStats < ActiveRecord::Base
  validates_uniqueness_of :job_id
end
