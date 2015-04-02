class JobStatsController < ApplicationController

  def index
    @job_stats = JobStats.all
  end

end
