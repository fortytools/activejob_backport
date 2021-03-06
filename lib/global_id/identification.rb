require 'active_support/concern'

class GlobalID
  module Identification
    extend ActiveSupport::Concern

    def to_global_id
      @global_id ||= GlobalID.create(self)
    end
    alias to_gid to_global_id

  end
end
