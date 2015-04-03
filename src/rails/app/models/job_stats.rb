class JobStats < ActiveRecord::Base
  validates_uniqueness_of [:job_id, :irc_id, :field_id]
end
