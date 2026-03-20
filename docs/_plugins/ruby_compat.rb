# Liquid/Jekyll 3.x still probes legacy taint APIs that were removed in newer
# Ruby releases. Reintroduce the old no-op surface so local docs builds remain
# reproducible on modern Homebrew Ruby versions.
unless "".respond_to?(:tainted?)
  class Object
    def tainted?
      false
    end

    def taint
      self
    end

    def untaint
      self
    end
  end
end
